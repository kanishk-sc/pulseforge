"""Batch Parquet files are visible to supported readers only after a commit manifest."""

import json

from botocore.exceptions import ClientError
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pulseforge.streaming.config import StreamSettings


def exists(storage, settings: StreamSettings, key: str) -> bool:
    try:
        storage.head_object(Bucket=settings.s3_bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise


def batch_path(settings: StreamSettings, layer: str, query_id: str, batch_id: int) -> str:
    return f"{settings.path(layer)}/{query_id}/batch-{batch_id:020d}"


def write_layer(frame: DataFrame, storage, settings: StreamSettings, path: str) -> None:
    key = path.removeprefix(f"s3a://{settings.s3_bucket}/") + "/_SUCCESS"
    if not exists(storage, settings, key):
        # Only uncommitted attempt directories are replaced. Successful batches are immutable.
        frame.write.mode("overwrite").parquet(path)


def curated(frame: DataFrame) -> DataFrame:
    return (
        frame.withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        .withColumn("is_payment_failure", F.col("event_type") == "payment_failed")
        .withColumn("is_shipment_delay", F.col("event_type") == "shipment_delayed")
        .withColumn("is_refund", F.col("event_type") == "refund_requested")
    )


def publish_manifest(
    storage, settings: StreamSettings, query_id: str, batch_id: int, row_count: int
) -> None:
    manifest = {
        "query_id": query_id,
        "batch_id": batch_id,
        "row_count": row_count,
        "cleaned": batch_path(settings, "cleaned", query_id, batch_id),
        "curated": batch_path(settings, "curated", query_id, batch_id),
    }
    storage.put_object(
        Bucket=settings.s3_bucket,
        Key=f"commits/{settings.stream_namespace}/{query_id}/{batch_id:020d}.json",
        Body=json.dumps(manifest, sort_keys=True).encode(),
        ContentType="application/json",
    )
