"""Quiesced Compose volume backup and separate-project restore; never overwrites volumes."""

import argparse
import hashlib
import json
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

VOLUMES = (
    "postgres-data",
    "kafka-data",
    "minio-data",
    "airflow-state",
    "redis-data",
    "model-cache",
    "prometheus-data",
    "grafana-data",
    "tempo-data",
)
REQUIRED = {"postgres-data", "kafka-data", "minio-data"}
PROJECT = re.compile(r"^pulseforge-[a-z0-9-]{3,40}$")


def docker(*args: str, capture: bool = True) -> str:
    result = subprocess.run(["docker", *args], check=True, text=True, capture_output=capture)
    return result.stdout.strip() if capture else ""


def check_project(project: str) -> None:
    if not PROJECT.fullmatch(project) or project == "pulseforge":
        raise ValueError("use_an_explicit_isolated_pulseforge_project")
    if docker("ps", "-q", "--filter", f"label=com.docker.compose.project={project}"):
        raise RuntimeError("stop_all_project_containers_before_volume_operation")


def volume_name(project: str, key: str) -> str:
    if key not in VOLUMES:
        raise ValueError("unknown_volume_key")
    return f"{project}_{key}"


def volume_exists(name: str) -> bool:
    return name in docker("volume", "ls", "-q", "--filter", f"name=^{name}$").splitlines()


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def backup(project: str, destination: Path) -> None:
    check_project(project)
    present = [key for key in VOLUMES if volume_exists(volume_name(project, key))]
    if not REQUIRED.issubset(present):
        raise RuntimeError("missing_required_platform_volume")
    for key in present:
        name = volume_name(project, key)
        labels = json.loads(docker("volume", "inspect", "--format", "{{json .Labels}}", name))
        if labels.get("com.docker.compose.project") != project:
            raise RuntimeError(f"volume_not_owned_by_project:{name}")
        if docker("ps", "-q", "--filter", f"volume={name}"):
            raise RuntimeError(f"volume_still_mounted_by_running_container:{name}")
    destination = destination.resolve()
    destination.mkdir(parents=True, exist_ok=False)
    archived: dict[str, dict[str, str | int]] = {}
    for key in present:
        name = volume_name(project, key)
        filename = f"{key}.tar"
        docker(
            "run",
            "--rm",
            "--mount",
            f"type=volume,source={name},target=/source,readonly",
            "--mount",
            f"type=bind,source={destination},target=/backup",
            "alpine:3.22",
            "tar",
            "-C",
            "/source",
            "-cf",
            f"/backup/{filename}",
            ".",
            capture=False,
        )
        archive = destination / filename
        archived[key] = {
            "file": filename,
            "sha256": digest(archive),
            "bytes": archive.stat().st_size,
        }
    manifest = {
        "schema_version": 1,
        "source_project": project,
        "created_at_utc": datetime.now(UTC).isoformat(),
        "consistency": "all project containers stopped before archive",
        "volumes": archived,
    }
    (destination / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {"source_project": project, "volumes": len(archived), "archive": str(destination)}
        )
    )


def restore(project: str, source: Path) -> None:
    check_project(project)
    if docker("ps", "-aq", "--filter", f"label=com.docker.compose.project={project}"):
        raise RuntimeError("target_project_already_has_containers")
    if any(volume_exists(volume_name(project, key)) for key in VOLUMES):
        raise RuntimeError("target_project_already_has_volumes")
    source = source.resolve()
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    entries = manifest["volumes"]
    if manifest.get("schema_version") != 1 or project == manifest.get("source_project"):
        raise RuntimeError("invalid_or_same_project_restore")
    if not REQUIRED.issubset(entries) or set(entries) - set(VOLUMES):
        raise RuntimeError("invalid_backup_volume_set")
    for key, entry in entries.items():
        if entry["file"] != f"{key}.tar" or volume_exists(volume_name(project, key)):
            raise RuntimeError(f"target_volume_exists_or_invalid:{key}")
        archive = source / entry["file"]
        if not archive.is_file() or digest(archive) != entry["sha256"]:
            raise RuntimeError(f"archive_checksum_mismatch:{key}")
    for key, entry in entries.items():
        name = volume_name(project, key)
        docker(
            "volume",
            "create",
            "--label",
            f"com.docker.compose.project={project}",
            "--label",
            f"com.docker.compose.volume={key}",
            name,
        )
        docker(
            "run",
            "--rm",
            "--mount",
            f"type=volume,source={name},target=/restore",
            "--mount",
            f"type=bind,source={source},target=/backup,readonly",
            "alpine:3.22",
            "tar",
            "-C",
            "/restore",
            "-xf",
            f"/backup/{entry['file']}",
            capture=False,
        )
    print(
        json.dumps(
            {
                "restored_project": project,
                "volumes": len(entries),
                "source_project": manifest["source_project"],
            }
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("backup", "restore"))
    parser.add_argument("--project", required=True)
    parser.add_argument("--archive", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "backup":
        backup(args.project, args.archive)
    else:
        restore(args.project, args.archive)


if __name__ == "__main__":
    main()
