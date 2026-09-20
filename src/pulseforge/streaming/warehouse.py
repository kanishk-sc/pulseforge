"""Durable idempotency and aggregates. Spark JDBC only writes retryable staging tables."""

import hashlib
import re
from importlib.resources import files

import psycopg
from psycopg import sql

from pulseforge.streaming.config import StreamSettings

EVENT_COLUMNS = (
    "schema_version",
    "event_id",
    "event_type",
    "event_ts",
    "customer_id",
    "order_id",
    "product_id",
    "amount",
    "currency",
    "payment_provider",
    "shipment_provider",
    "region",
    "status",
    "metadata",
    "source_topic",
    "source_partition",
    "source_offset",
    "source_timestamp",
    "ingested_at",
)


def stage_name(query_id: str, batch_id: int) -> str:
    if batch_id < 0 or not query_id:
        raise ValueError("query identity and nonnegative batch ID required")
    return "stage_" + hashlib.sha256(f"{query_id}:{batch_id}".encode()).hexdigest()[:24]


def checked_table(name: str) -> str:
    if not re.fullmatch(r"stage_[0-9a-f]{24}", name):
        raise ValueError("unsafe staging table name")
    return name


def connect(settings: StreamSettings) -> psycopg.Connection:
    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        connect_timeout=5,
        options="-c timezone=UTC -c statement_timeout=30000 -c lock_timeout=10000",
        autocommit=True,
    )


def initialize_schema(connection: psycopg.Connection) -> None:
    with connection.transaction():
        connection.execute(files("pulseforge.streaming").joinpath("warehouse.sql").read_text())


def prepare_stage(connection: psycopg.Connection, name: str) -> None:
    name = checked_table(name)
    with connection.transaction():
        connection.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(name)))
        connection.execute(
            sql.SQL("CREATE TABLE {} (LIKE stream_event_stage)").format(sql.Identifier(name))
        )


def candidate_query(name: str) -> str:
    name = checked_table(name)
    # All identifiers are fixed or hash-validated. DISTINCT absorbs JDBC task retry duplicates.
    return f"""SELECT DISTINCT ON (s.event_id) s.* FROM {name} s
        WHERE NOT EXISTS (SELECT 1 FROM stream_events e WHERE e.event_id = s.event_id::uuid)
          AND NOT EXISTS (SELECT 1 FROM stream_events e WHERE
            (e.source_topic,e.source_partition,e.source_offset) =
            (s.source_topic,s.source_partition,s.source_offset))
        ORDER BY s.event_id,s.source_topic,s.source_partition,s.source_offset"""


def committed(connection: psycopg.Connection, query_id: str, batch_id: int) -> tuple | None:
    return connection.execute(
        "SELECT row_count FROM streaming_batches WHERE query_id=%s AND batch_id=%s",
        (query_id, batch_id),
    ).fetchone()


def commit_stage(connection: psycopg.Connection, name: str, query_id: str, batch_id: int) -> int:
    checked_table(name)
    with connection.transaction():
        prior = committed(connection, query_id, batch_id)
        if prior:
            return prior[0]
        columns = ",".join(EVENT_COLUMNS)
        projection = ",".join(
            "event_id::uuid"
            if col == "event_id"
            else "metadata::jsonb"
            if col == "metadata"
            else col
            for col in EVENT_COLUMNS
        )
        result = connection.execute(f"""
            WITH inserted AS (
                INSERT INTO stream_events ({columns})
                SELECT {projection} FROM ({candidate_query(name)}) candidates
                ON CONFLICT DO NOTHING RETURNING event_ts,event_type,amount
            ), totals AS (
                SELECT date_trunc('minute',event_ts) AS window_start, event_type,
                       count(*) AS event_count, coalesce(sum(amount),0) AS total_amount
                FROM inserted GROUP BY 1,2
            ), metrics AS (
                INSERT INTO stream_metrics_minute
                    (window_start,window_end,event_type,event_count,total_amount)
                SELECT window_start,window_start+interval '1 minute',event_type,
                       event_count,total_amount FROM totals
                ON CONFLICT (window_start,event_type) DO UPDATE SET
                    event_count=stream_metrics_minute.event_count+excluded.event_count,
                    total_amount=stream_metrics_minute.total_amount+excluded.total_amount,
                    updated_at=now()
            ) SELECT count(*) FROM inserted
        """).fetchone()[0]
        connection.execute(
            "INSERT INTO streaming_batches(query_id,batch_id,row_count) VALUES (%s,%s,%s)",
            (query_id, batch_id, result),
        )
        connection.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(name)))
        return result
