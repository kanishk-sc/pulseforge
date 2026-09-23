import json
import logging
from contextlib import closing

import psycopg
from botocore.exceptions import BotoCoreError, ClientError
from pyspark.sql import DataFrame

from pulseforge.dependencies import s3_client
from pulseforge.streaming.config import StreamSettings
from pulseforge.streaming.lake import batch_path, curated, publish_manifest, write_layer
from pulseforge.streaming.telemetry import SINK_FAILURES, SINK_INPUT_ROWS, SINK_INSERTED_ROWS
from pulseforge.streaming.warehouse import (
    EVENT_COLUMNS,
    candidate_query,
    commit_stage,
    committed,
    connect,
    prepare_stage,
    stage_name,
)

logger = logging.getLogger(__name__)


def write_batch(frame: DataFrame, batch_id: int, settings: StreamSettings) -> None:
    frame.persist()
    try:
        count = frame.count()  # Fully consume stateful output, even on a committed retry.
        with closing(s3_client(settings)) as storage, connect(settings) as connection:
            # The checkpoint UUID distinguishes a new generation from an ordinary restart.
            key = (
                settings.checkpoint("valid-events").removeprefix(f"s3a://{settings.s3_bucket}/")
                + "/metadata"
            )
            response = storage.get_object(Bucket=settings.s3_bucket, Key=key)
            with response["Body"] as body:
                query_id = json.loads(body.read())["id"]
            # Single active warehouse writer; hold a session lock through both lake writes.
            if not connection.execute("SELECT pg_try_advisory_lock(72419021)").fetchone()[0]:
                raise RuntimeError("Another streaming sink owns the warehouse writer lock")
            prior = committed(connection, query_id, batch_id)
            if prior:
                publish_manifest(storage, settings, query_id, batch_id, prior[0])
                return
            name = stage_name(query_id, batch_id)
            prepare_stage(connection, name)
            properties = {
                "user": settings.postgres_user,
                "password": settings.postgres_password.get_secret_value(),
                "driver": "org.postgresql.Driver",
            }
            if count:
                frame.select(*EVENT_COLUMNS).write.jdbc(
                    settings.jdbc_url, name, mode="append", properties=properties
                )
            candidates = frame.sparkSession.read.jdbc(
                settings.jdbc_url, f"({candidate_query(name)}) candidates", properties=properties
            ).persist()
            try:
                write_layer(
                    candidates,
                    storage,
                    settings,
                    batch_path(settings, "cleaned", query_id, batch_id),
                )
                write_layer(
                    curated(candidates),
                    storage,
                    settings,
                    batch_path(settings, "curated", query_id, batch_id),
                )
                inserted = commit_stage(connection, name, query_id, batch_id)
                publish_manifest(storage, settings, query_id, batch_id, inserted)
                logger.info(
                    "stream_batch_committed batch=%s input=%s inserted=%s",
                    batch_id,
                    count,
                    inserted,
                )
                try:
                    SINK_INPUT_ROWS.inc(count)
                    SINK_INSERTED_ROWS.inc(inserted)
                except Exception:
                    logger.warning("sink_telemetry_failed")
            finally:
                candidates.unpersist()
    except Exception as exc:
        dependency = (
            "database"
            if isinstance(exc, psycopg.Error)
            else "lake"
            if isinstance(exc, (BotoCoreError, ClientError))
            else "other"
        )
        try:
            SINK_FAILURES.labels(dependency).inc()
        except Exception:
            logger.warning("sink_telemetry_failed")
        raise
    finally:
        frame.unpersist()
