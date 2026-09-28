"""Build and verify commit-tagged local release images without publishing them."""

import argparse
import json
import os
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OVERLAY = ROOT / "infra/docker/compose.release.yml"
IMAGES = ("python", "streaming", "dbt", "airflow", "dashboard", "minio")
BUILD_SERVICES = ("api", "streaming", "analytics-dbt", "airflow", "dashboard", "minio")
LOCAL_IMAGE_SERVICES = {
    "minio": "minio",
    "bootstrap": "python",
    "api": "python",
    "streaming": "streaming",
    "producer": "python",
    "analytics-dbt": "dbt",
    "airflow": "airflow",
    "dashboard": "dashboard",
    "product-migrate": "python",
    "detector": "python",
    "assistant-model": "python",
    "assistant-migrate": "python",
    "assistant-ingest": "python",
    "ops-exporter": "python",
}
PROJECT_PATTERN = re.compile(r"^pulseforge-[a-z0-9-]{3,40}$")


def capture(*args: str, env: dict[str, str] | None = None) -> str:
    return subprocess.check_output(args, cwd=ROOT, env=env, text=True).strip()


def source_sha() -> str:
    sha = capture("git", "rev-parse", "HEAD")
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise RuntimeError("not_a_full_git_commit")
    if capture("git", "status", "--porcelain=v1"):
        raise RuntimeError("release_requires_clean_worktree")
    return sha


def compose_args(project: str) -> list[str]:
    if not PROJECT_PATTERN.fullmatch(project) or project == "pulseforge":
        raise ValueError("release_project_must_be_a_separate_pulseforge-name")
    return [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(ROOT / "docker-compose.yml"),
        "-f",
        str(OVERLAY),
    ]


def release_environment(sha: str) -> dict[str, str]:
    env = os.environ.copy()
    env["PULSEFORGE_RELEASE_SHA"] = sha
    return env


def verify_configuration(project: str, sha: str) -> None:
    # Compose resolves credentials in this output. Never echo it, even on failure.
    config = json.loads(
        capture(
            *compose_args(project),
            "--profile",
            "*",
            "config",
            "--format",
            "json",
            env=release_environment(sha),
        )
    )
    for name, service in config["services"].items():
        if service.get("network_mode") == "host" or service.get("privileged"):
            raise RuntimeError(f"unsafe_container_network_or_privilege:{name}")
        for port in service.get("ports", []):
            if port.get("host_ip") != "127.0.0.1":
                raise RuntimeError(f"non_loopback_port:{name}")
    for name, image in LOCAL_IMAGE_SERVICES.items():
        service = config["services"].get(name, {})
        if (
            service.get("image") != f"pulseforge-{image}:{sha}"
            or service.get("pull_policy") != "never"
        ):
            raise RuntimeError(f"unverified_release_image_config:{name}")
    for name in ("streaming", "producer", "analytics-dbt", "airflow"):
        if config["services"][name].get("deploy", {}).get("replicas") != 1:
            raise RuntimeError(f"invalid_singleton_count:{name}")
    if config["services"]["api"]["environment"].get("ASSISTANT_PROVIDER") != "disabled":
        raise RuntimeError("provider_must_remain_disabled_for_release_acceptance")
    if config["services"]["api"]["environment"].get("ASSISTANT_PROVIDER_KEY"):
        raise RuntimeError("provider_key_must_not_be_injected_into_private_release")


def image_identity(name: str, sha: str) -> dict[str, str]:
    try:
        data = json.loads(capture("docker", "image", "inspect", f"pulseforge-{name}:{sha}"))[0]
    except (subprocess.CalledProcessError, json.JSONDecodeError, IndexError) as exc:
        raise RuntimeError(f"missing_or_invalid_release_image:{name}") from exc
    if data["Architecture"] != "amd64" or data["Os"] != "linux":
        raise RuntimeError(f"unsupported_image_architecture:{name}")
    return {"image": f"pulseforge-{name}:{sha}", "id": data["Id"]}


def build(project: str, manifest: Path) -> None:
    sha = source_sha()
    verify_configuration(project, sha)
    subprocess.run(
        [*compose_args(project), "build", *BUILD_SERVICES],
        cwd=ROOT,
        env=release_environment(sha),
        check=True,
    )
    payload = {
        "schema_version": 1,
        "source_commit": sha,
        "built_at_utc": datetime.now(UTC).isoformat(),
        "project": project,
        "images": {name: image_identity(name, sha) for name in IMAGES},
        "published_externally": False,
    }
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"source_commit": sha, "images": len(IMAGES), "manifest": str(manifest)}))


def verify(project: str, manifest: Path) -> None:
    sha = source_sha()
    verify_configuration(project, sha)
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    if (
        payload.get("schema_version") != 1
        or set(payload.get("images", {})) != set(IMAGES)
        or payload.get("published_externally") is not False
    ):
        raise RuntimeError("invalid_release_manifest")
    if payload.get("source_commit") != sha or payload.get("project") != project:
        raise RuntimeError("manifest_source_or_project_mismatch")
    for name in IMAGES:
        if payload["images"][name] != image_identity(name, sha):
            raise RuntimeError(f"image_identity_mismatch:{name}")
    print(json.dumps({"verified_commit": sha, "images": len(IMAGES), "project": project}))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("build", "verify", "config-check"))
    parser.add_argument("--project", required=True)
    parser.add_argument("--manifest", type=Path, default=ROOT / ".pytest_cache/phase7-release.json")
    args = parser.parse_args()
    if args.command == "build":
        build(args.project, args.manifest)
    elif args.command == "verify":
        verify(args.project, args.manifest)
    else:
        sha = source_sha()
        verify_configuration(args.project, sha)
        print(json.dumps({"config_verified_commit": sha, "project": args.project}))


if __name__ == "__main__":
    main()
