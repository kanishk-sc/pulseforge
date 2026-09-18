import os
from datetime import UTC, datetime, timedelta

import boto3
import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="pulseforge_lake_retention",
    description="Delete expired raw/cleaned objects in bounded batches; dry-run by default.",
    schedule="30 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["pulseforge", "retention"],
)
def lake_retention():
    @task
    def remove_expired_objects() -> dict[str, int | bool]:
        retention_days = max(1, int(os.getenv("LAKE_RETENTION_DAYS", "30")))
        dry_run = os.getenv("LAKE_RETENTION_DRY_RUN", "true").lower() != "false"
        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        client = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT_URL"],
            aws_access_key_id=os.environ["MINIO_ROOT_USER"],
            aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
            region_name="us-east-1",
        )
        bucket = os.getenv("S3_BUCKET", "pulseforge")
        candidates = []
        for prefix in ("raw/stream_events/", "cleaned/stream_events/"):
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                candidates.extend(
                    {"Key": item["Key"]}
                    for item in page.get("Contents", [])
                    if item["LastModified"] < cutoff and not item["Key"].endswith("/")
                )
        deleted = 0
        if not dry_run:
            for start in range(0, len(candidates), 1000):
                response = client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": candidates[start : start + 1000], "Quiet": True},
                )
                deleted += len(candidates[start : start + 1000]) - len(response.get("Errors", []))
        return {"dry_run": dry_run, "candidates": len(candidates), "deleted": deleted}

    remove_expired_objects()


lake_retention()
