import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import psycopg

from pulseforge.product.detectors import evaluate
from pulseforge.product.publication import (
    begin_build,
    fail_build,
    publish_build,
    successful_build_for_key,
)
from pulseforge.product.schema import apply_migrations


def dsn() -> str:
    return (
        f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
        f"port={os.getenv('POSTGRES_PORT', '5432')} "
        f"dbname={os.getenv('POSTGRES_DB', 'pulseforge')} "
        f"user={os.getenv('POSTGRES_USER', 'pulseforge')} "
        f"password={os.getenv('POSTGRES_PASSWORD', '')} connect_timeout=5"
    )


def pipeline(
    build_key: str,
    project_dir: Path,
    profiles_dir: Path,
    detector_now: datetime | None = None,
) -> int:
    with psycopg.connect(dsn()) as connection:
        apply_migrations(connection)
        published_build = successful_build_for_key(connection, build_key)
        if published_build:
            created = evaluate(connection, detector_now)
            print(f"reused published build={published_build} incidents={len(created)}")
            return 0
        build_id = begin_build(connection, build_key)
        try:
            subprocess.run(
                [
                    "dbt",
                    "build",
                    "--project-dir",
                    str(project_dir),
                    "--profiles-dir",
                    str(profiles_dir),
                    "--target",
                    "dev",
                ],
                check=True,
            )
            publish_build(connection, build_id, project_dir / "target" / "run_results.json")
            created = evaluate(connection, detector_now)
            print(f"published build={build_id} incidents={len(created)}")
            return 0
        except Exception as exc:
            fail_build(connection, build_id, f"{type(exc).__name__}: {exc}")
            raise


def main() -> int:
    parser = argparse.ArgumentParser(description="PulseForge product database operations")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("migrate")
    detect = sub.add_parser("detect")
    detect.add_argument("--now")
    run = sub.add_parser("pipeline")
    run.add_argument("--build-key", required=True)
    run.add_argument("--project-dir", type=Path, default=Path("analytics"))
    run.add_argument("--profiles-dir", type=Path, default=Path("analytics"))
    run.add_argument(
        "--detector-now",
        type=datetime.fromisoformat,
        help="fixed evaluation clock for deterministic acceptance runs",
    )
    args = parser.parse_args()
    with psycopg.connect(dsn()) as connection:
        if args.command == "migrate":
            print("applied migrations:", ", ".join(apply_migrations(connection)) or "none")
            return 0
        if args.command == "detect":
            apply_migrations(connection)
            now = datetime.fromisoformat(args.now) if args.now else None
            print("created incidents:", len(evaluate(connection, now)))
            return 0
    return pipeline(args.build_key, args.project_dir, args.profiles_dir, args.detector_now)


if __name__ == "__main__":
    sys.exit(main())
