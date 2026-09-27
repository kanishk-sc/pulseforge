"""Credential-free guards for local release and recovery tooling."""

import importlib.util
import json
from pathlib import Path

import pytest


def load_script(name: str):
    path = Path(__file__).resolve().parents[1] / "scripts/deployment" / name
    spec = importlib.util.spec_from_file_location(name.replace(".py", ""), path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_release_rejects_dirty_source_and_wrong_project(monkeypatch):
    release = load_script("release.py")
    monkeypatch.setattr(
        release, "capture", lambda *args, **kwargs: "a" * 40 if "rev-parse" in args else " M file"
    )
    with pytest.raises(RuntimeError, match="clean_worktree"):
        release.source_sha()
    with pytest.raises(ValueError, match="separate"):
        release.compose_args("pulseforge")
    with pytest.raises(ValueError, match="separate"):
        release.compose_args("other-project")


def test_release_config_requires_loopback_singletons_and_disabled_provider(monkeypatch):
    release = load_script("release.py")
    services = {
        name: {"ports": [], "deploy": {"replicas": 1}, "environment": {}}
        for name in ("streaming", "producer", "analytics-dbt", "airflow")
    }
    services["api"] = {
        "ports": [{"host_ip": "127.0.0.1"}],
        "environment": {"ASSISTANT_PROVIDER": "disabled"},
    }
    monkeypatch.setattr(
        release, "capture", lambda *args, **kwargs: json.dumps({"services": services})
    )
    release.verify_configuration("pulseforge-p7-test", "a" * 40)
    services["api"]["ports"][0]["host_ip"] = "0.0.0.0"
    with pytest.raises(RuntimeError, match="non_loopback"):
        release.verify_configuration("pulseforge-p7-test", "a" * 40)
    services["api"]["ports"][0]["host_ip"] = "127.0.0.1"
    services["streaming"]["deploy"]["replicas"] = 2
    with pytest.raises(RuntimeError, match="invalid_singleton"):
        release.verify_configuration("pulseforge-p7-test", "a" * 40)
    services["streaming"]["deploy"]["replicas"] = 1
    services["api"]["environment"]["ASSISTANT_PROVIDER"] = "openai"
    with pytest.raises(RuntimeError, match="provider_must_remain_disabled"):
        release.verify_configuration("pulseforge-p7-test", "a" * 40)


def test_recovery_rejects_running_project_and_archive_tampering(monkeypatch, tmp_path):
    recovery = load_script("recovery.py")
    monkeypatch.setattr(recovery, "docker", lambda *args, **kwargs: "container-id")
    with pytest.raises(RuntimeError, match="stop_all_project"):
        recovery.check_project("pulseforge-p7-test")
    with pytest.raises(ValueError, match="isolated"):
        recovery.check_project("pulseforge")

    source = tmp_path / "backup"
    source.mkdir()
    volumes = {}
    for key in recovery.REQUIRED:
        archive = source / f"{key}.tar"
        archive.write_bytes(b"valid")
        volumes[key] = {"file": archive.name, "sha256": recovery.digest(archive), "bytes": 5}
    (source / "manifest.json").write_text(
        json.dumps(
            {"schema_version": 1, "source_project": "pulseforge-p7-source", "volumes": volumes}
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(recovery, "docker", lambda *args, **kwargs: "")
    monkeypatch.setattr(recovery, "volume_exists", lambda *_args: False)
    (source / "postgres-data.tar").write_bytes(b"tampered")
    with pytest.raises(RuntimeError, match="archive_checksum_mismatch"):
        recovery.restore("pulseforge-p7-target", source)


def test_recovery_restores_only_new_project_volumes(monkeypatch, tmp_path):
    recovery = load_script("recovery.py")
    source = tmp_path / "backup"
    source.mkdir()
    volumes = {}
    for key in recovery.REQUIRED:
        archive = source / f"{key}.tar"
        archive.write_bytes(b"valid")
        volumes[key] = {"file": archive.name, "sha256": recovery.digest(archive), "bytes": 5}
    (source / "manifest.json").write_text(
        json.dumps(
            {"schema_version": 1, "source_project": "pulseforge-p7-source", "volumes": volumes}
        ),
        encoding="utf-8",
    )
    calls = []
    monkeypatch.setattr(recovery, "docker", lambda *args, **kwargs: calls.append(args) or "")
    monkeypatch.setattr(recovery, "volume_exists", lambda *_args: False)
    recovery.restore("pulseforge-p7-target", source)
    assert sum(args[:2] == ("volume", "create") for args in calls) == len(recovery.REQUIRED)
    assert all(
        "pulseforge-p7-target" in " ".join(args)
        for args in calls
        if args[:2] == ("volume", "create")
    )
