"""Read-only, representative PostgreSQL and MinIO snapshot for isolated restore checks."""

import argparse
import hashlib
import json
import re
from pathlib import Path

import boto3
import psycopg
from botocore.config import Config
from dotenv import dotenv_values

PROJECT = re.compile(r"^pulseforge-p7-[a-z0-9-]{3,40}$")
PREFIXES = ("raw/", "cleaned/", "curated/", "commits/", "checkpoints/")


def representative_candidate(prefix: str, key: str) -> bool:
    if prefix in {"raw/", "cleaned/", "curated/"}:
        return key.endswith(".parquet")
    if prefix == "commits/":
        return key.endswith(".json")
    return "/commits/" in key


def snapshot(env_file: Path, project: str) -> dict:
    if not PROJECT.fullmatch(project):
        raise ValueError("probe_requires_an_explicit_phase7_project")
    values = dotenv_values(env_file)
    password = values.get("POSTGRES_PASSWORD")
    minio_password = values.get("MINIO_ROOT_PASSWORD")
    if not password or not minio_password:
        raise ValueError("missing_credentials_in_operator_env_file")
    with psycopg.connect(
        host="127.0.0.1",
        port=15432,
        dbname=values.get("POSTGRES_DB") or "pulseforge",
        user=values.get("POSTGRES_USER") or "pulseforge",
        password=password,
        connect_timeout=5,
    ) as connection:
        events = connection.execute(
            "SELECT count(*), count(DISTINCT event_id), "
            "count(DISTINCT (source_topic, source_partition, source_offset)), "
            "coalesce(sum(amount), 0)::text FROM stream_events"
        ).fetchone()
        if events[0] != events[1] or events[0] != events[2]:
            raise RuntimeError("duplicate_event_or_source_identity")
        batches = connection.execute(
            "SELECT count(*), coalesce(sum(row_count), 0) FROM streaming_batches"
        ).fetchone()
        build = connection.execute(
            "SELECT build_id::text, build_key, source_event_count, manifest_sha256 "
            "FROM product.analytics_builds WHERE status='succeeded' "
            "ORDER BY published_at DESC LIMIT 1"
        ).fetchone()
        if build is None:
            raise RuntimeError("missing_successful_publication")
        marts = connection.execute(
            "SELECT coalesce(sum(payment_attempt_count), 0), "
            "coalesce(sum(failed_payment_count), 0), "
            "coalesce(sum(revenue_amount), 0)::text "
            "FROM product.operations_health_hourly WHERE build_id=%s",
            (build[0],),
        ).fetchone()
        incidents = connection.execute(
            "SELECT incident_id::text, analytics_build_id::text, detector_name, "
            "observed_metric::text, baseline_metric::text, threshold::text "
            "FROM product.incidents ORDER BY incident_id"
        ).fetchall()
        evidence = connection.execute(
            "SELECT incident_id::text, source_event_id::text, evidence_role "
            "FROM product.incident_evidence ORDER BY incident_id, source_event_id"
        ).fetchall()
        corpus = connection.execute(
            "SELECT (SELECT count(*) FROM assistant.documents), "
            "(SELECT count(*) FROM assistant.chunks), "
            "(SELECT corpus_sha256 FROM assistant.index_state WHERE singleton=true)"
        ).fetchone()
    storage = boto3.client(
        "s3",
        endpoint_url="http://127.0.0.1:19000",
        aws_access_key_id=values.get("MINIO_ROOT_USER") or "pulseforge",
        aws_secret_access_key=minio_password,
        config=Config(s3={"addressing_style": "path"}, connect_timeout=5, read_timeout=10),
    )
    bucket = values.get("S3_BUCKET") or "pulseforge"
    objects = {}
    for prefix in PREFIXES:
        count = 0
        representative = None
        for page in storage.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                count += 1
                if (
                    representative is None
                    and item["Size"] > 0
                    and representative_candidate(prefix, item["Key"])
                ):
                    key = item["Key"]
                    body = storage.get_object(Bucket=bucket, Key=key)["Body"]
                    try:
                        digest = hashlib.sha256(body.read()).hexdigest()
                    finally:
                        body.close()
                    representative = {"key": key, "sha256": digest, "bytes": item["Size"]}
        if not count or representative is None:
            raise RuntimeError(f"missing_nonempty_object:{prefix}")
        objects[prefix] = {"count": count, "representative": representative}
    return {
        "events": events,
        "batches": batches,
        "successful_build": build,
        "published_marts": marts,
        "incidents": incidents,
        "incident_evidence": evidence,
        "corpus": corpus,
        "objects": objects,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--compare", type=Path)
    args = parser.parse_args()
    result = json.loads(json.dumps(snapshot(args.env_file, args.project), default=str))
    if args.compare:
        expected = json.loads(args.compare.read_text(encoding="utf-8"))
        if result != expected:
            differences = sorted(key for key in result if result[key] != expected.get(key))
            raise RuntimeError(f"restored_state_differs:{','.join(differences)}")
    if args.output:
        if args.output.exists():
            raise FileExistsError("snapshot_output_already_exists")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "project": args.project,
                "events": result["events"][0],
                "batches": result["batches"][0],
                "build_id": result["successful_build"][0],
                "incidents": len(result["incidents"]),
                "evidence": len(result["incident_evidence"]),
                "corpus_chunks": result["corpus"][1],
                "object_classes": len(result["objects"]),
                "matched": args.compare is not None,
            }
        )
    )


if __name__ == "__main__":
    main()
