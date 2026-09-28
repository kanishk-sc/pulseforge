"""Credential-free guards for local release and recovery tooling."""

import importlib.util
import json
import subprocess
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
        name: {
            "ports": [],
            "deploy": {"replicas": 1},
            "environment": {},
            "image": f"pulseforge-{image}:{'a' * 40}",
            "pull_policy": "never",
        }
        for name, image in release.LOCAL_IMAGE_SERVICES.items()
    }
    services["api"]["ports"] = [{"host_ip": "127.0.0.1"}]
    services["api"]["environment"] = {
        "ASSISTANT_PROVIDER": "disabled",
        "ASSISTANT_PROVIDER_KEY": "",
    }
    monkeypatch.setattr(
        release, "capture", lambda *args, **kwargs: json.dumps({"services": services})
    )
    release.verify_configuration("pulseforge-p7-test", "a" * 40)
    services["dashboard"]["pull_policy"] = "missing"
    with pytest.raises(RuntimeError, match="unverified_release_image_config:dashboard"):
        release.verify_configuration("pulseforge-p7-test", "a" * 40)
    services["dashboard"]["pull_policy"] = "never"
    services["dashboard"]["image"] = f"pulseforge-dashboard:{'b' * 40}"
    with pytest.raises(RuntimeError, match="unverified_release_image_config:dashboard"):
        release.verify_configuration("pulseforge-p7-test", "a" * 40)
    services["dashboard"]["image"] = f"pulseforge-dashboard:{'a' * 40}"
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
    services["api"]["environment"]["ASSISTANT_PROVIDER"] = "disabled"
    services["api"]["environment"]["ASSISTANT_PROVIDER_KEY"] = "not-allowed"
    with pytest.raises(RuntimeError, match="provider_key_must_not_be_injected"):
        release.verify_configuration("pulseforge-p7-test", "a" * 40)
    services["api"]["environment"]["ASSISTANT_PROVIDER_KEY"] = ""
    services["api"]["network_mode"] = "host"
    with pytest.raises(RuntimeError, match="unsafe_container_network"):
        release.verify_configuration("pulseforge-p7-test", "a" * 40)


def test_release_verify_rejects_missing_and_mixed_images(monkeypatch, tmp_path):
    release = load_script("release.py")
    sha = "a" * 40
    monkeypatch.setattr(release, "source_sha", lambda: sha)
    monkeypatch.setattr(release, "verify_configuration", lambda *_args: None)
    manifest = tmp_path / "release.json"
    payload = {
        "schema_version": 1,
        "source_commit": sha,
        "project": "pulseforge-p7-test",
        "images": {
            name: {"image": f"pulseforge-{name}:{sha}", "id": name} for name in release.IMAGES
        },
        "published_externally": False,
    }
    manifest.write_text(json.dumps({**payload, "images": {}}), encoding="utf-8")
    with pytest.raises(RuntimeError, match="invalid_release_manifest"):
        release.verify("pulseforge-p7-test", manifest)
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    real_image_identity = release.image_identity
    monkeypatch.setattr(
        release,
        "image_identity",
        lambda name, sha: {"image": f"pulseforge-{name}:{sha}", "id": "other"},
    )
    with pytest.raises(RuntimeError, match="image_identity_mismatch"):
        release.verify("pulseforge-p7-test", manifest)
    monkeypatch.setattr(release, "image_identity", real_image_identity)
    monkeypatch.setattr(
        release,
        "capture",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(subprocess.CalledProcessError(1, "docker")),
    )
    with pytest.raises(RuntimeError, match="missing_or_invalid_release_image:python"):
        release.image_identity("python", sha)


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
    monkeypatch.setattr(recovery, "volume_exists", lambda name: name.endswith("_grafana-data"))
    with pytest.raises(RuntimeError, match="target_project_already_has_volumes"):
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


def test_state_probe_refuses_non_isolated_project():
    probe = load_script("state_probe.py")
    with pytest.raises(ValueError, match="explicit_phase7_project"):
        probe.snapshot(Path(".env"), "pulseforge")
    assert not probe.representative_candidate("raw/", "raw/v1/_spark_metadata/0")
    assert probe.representative_candidate("raw/", "raw/v1/part-0.parquet")
    assert probe.representative_candidate("checkpoints/", "checkpoints/x/commits/0")
