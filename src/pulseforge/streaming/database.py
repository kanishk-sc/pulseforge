from collections.abc import Iterable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pulseforge.streaming.settings import StreamingSettings

DDL = """
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.stream_events (
    event_id uuid PRIMARY KEY,
    schema_version smallint NOT NULL,
    event_type text NOT NULL,
    event_timestamp timestamptz NOT NULL,
    customer_id text,
    order_id text,
    product_id text,
    amount numeric(14, 2),
    currency text NOT NULL,
    payment_provider text,
    shipment_provider text,
    region text NOT NULL,
    status text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_topic text NOT NULL,
    source_partition integer NOT NULL,
    source_offset bigint NOT NULL,
    kafka_timestamp timestamptz NOT NULL,
    processed_at timestamptz NOT NULL,
    processing_latency_ms bigint NOT NULL,
    raw_value text NOT NULL,
    UNIQUE (source_topic, source_partition, source_offset)
);

CREATE INDEX IF NOT EXISTS ix_stream_events_timestamp
    ON analytics.stream_events (event_timestamp DESC);
CREATE INDEX IF NOT EXISTS ix_stream_events_type_timestamp
    ON analytics.stream_events (event_type, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS ix_stream_events_order_id
    ON analytics.stream_events (order_id) WHERE order_id IS NOT NULL;

CREATE UNLOGGED TABLE IF NOT EXISTS analytics.stream_events_staging (
    event_id text NOT NULL,
    schema_version smallint NOT NULL,
    event_type text NOT NULL,
    event_timestamp timestamptz NOT NULL,
    customer_id text,
    order_id text,
    product_id text,
    amount numeric(14, 2),
    currency text NOT NULL,
    payment_provider text,
    shipment_provider text,
    region text NOT NULL,
    status text NOT NULL,
    metadata_json text,
    source_topic text NOT NULL,
    source_partition integer NOT NULL,
    source_offset bigint NOT NULL,
    kafka_timestamp timestamptz NOT NULL,
    processed_at timestamptz NOT NULL,
    processing_latency_ms bigint NOT NULL,
    raw_value text NOT NULL,
    batch_id bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_stream_events_staging_batch
    ON analytics.stream_events_staging (batch_id);

CREATE TABLE IF NOT EXISTS analytics.stream_metrics_minute (
    minute_start timestamptz NOT NULL,
    minute_end timestamptz NOT NULL,
    region text NOT NULL,
    event_count bigint NOT NULL,
    order_count bigint NOT NULL,
    successful_payment_count bigint NOT NULL,
    failed_payment_count bigint NOT NULL,
    payment_attempt_count bigint NOT NULL,
    successful_revenue numeric(18, 2) NOT NULL,
    payment_failure_rate double precision NOT NULL,
    shipment_created_count bigint NOT NULL,
    shipment_delayed_count bigint NOT NULL,
    refund_request_count bigint NOT NULL,
    avg_processing_latency_ms double precision,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (minute_start, region)
);

CREATE UNLOGGED TABLE IF NOT EXISTS analytics.stream_metrics_minute_staging
    (LIKE analytics.stream_metrics_minute INCLUDING DEFAULTS EXCLUDING CONSTRAINTS);
ALTER TABLE analytics.stream_metrics_minute_staging
    ADD COLUMN IF NOT EXISTS batch_id bigint;
CREATE INDEX IF NOT EXISTS ix_stream_metrics_staging_batch
    ON analytics.stream_metrics_minute_staging (batch_id);
"""

EVENT_SOURCE_COLUMNS = (
    "event_id",
    "schema_version",
    "event_type",
    "event_timestamp",
    "customer_id",
    "order_id",
    "product_id",
    "amount",
    "currency",
    "payment_provider",
    "shipment_provider",
    "region",
    "status",
    "metadata_json",
    "source_topic",
    "source_partition",
    "source_offset",
    "kafka_timestamp",
    "processed_at",
    "processing_latency_ms",
    "raw_value",
)

EVENT_TARGET_COLUMNS = tuple(
    "metadata" if column == "metadata_json" else column for column in EVENT_SOURCE_COLUMNS
)

METRIC_COLUMNS = (
    "minute_start",
    "minute_end",
    "region",
    "event_count",
    "order_count",
    "successful_payment_count",
    "failed_payment_count",
    "payment_attempt_count",
    "successful_revenue",
    "payment_failure_rate",
    "shipment_created_count",
    "shipment_delayed_count",
    "refund_request_count",
    "avg_processing_latency_ms",
    "updated_at",
)


def _connect(settings: StreamingSettings):
    import psycopg

    return psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password,
        dbname=settings.postgres_db,
        connect_timeout=10,
    )


def ensure_tables(settings: StreamingSettings) -> None:
    with _connect(settings) as connection:
        connection.execute(DDL)


def _jdbc_append(frame: DataFrame, table: str, settings: StreamingSettings) -> None:
    (
        frame.write.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", table)
        .option("user", settings.postgres_user)
        .option("password", settings.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def _column_list(columns: Iterable[str]) -> str:
    return ", ".join(columns)


def write_event_batch(frame: DataFrame, batch_id: int, settings: StreamingSettings) -> None:
    if frame.isEmpty():
        return
    with _connect(settings) as connection:
        connection.execute(
            "DELETE FROM analytics.stream_events_staging WHERE batch_id = %s", (batch_id,)
        )
    _jdbc_append(
        frame.withColumn("batch_id", F.lit(batch_id)),
        "analytics.stream_events_staging",
        settings,
    )
    target_columns = _column_list(EVENT_TARGET_COLUMNS)
    source_columns = _column_list(
        "event_id::uuid"
        if column == "event_id"
        else "metadata_json::jsonb"
        if column == "metadata_json"
        else column
        for column in EVENT_SOURCE_COLUMNS
    )
    with _connect(settings) as connection:
        with connection.transaction():
            connection.execute(
                f"""
                INSERT INTO analytics.stream_events ({target_columns})
                SELECT {source_columns}
                FROM analytics.stream_events_staging
                WHERE batch_id = %s
                ON CONFLICT DO NOTHING
                """,
                (batch_id,),
            )
            connection.execute(
                "DELETE FROM analytics.stream_events_staging WHERE batch_id = %s", (batch_id,)
            )


def write_metric_batch(frame: DataFrame, batch_id: int, settings: StreamingSettings) -> None:
    if frame.isEmpty():
        return
    with _connect(settings) as connection:
        connection.execute(
            "DELETE FROM analytics.stream_metrics_minute_staging WHERE batch_id = %s",
            (batch_id,),
        )
    _jdbc_append(
        frame.withColumn("batch_id", F.lit(batch_id)),
        "analytics.stream_metrics_minute_staging",
        settings,
    )
    columns = _column_list(METRIC_COLUMNS)
    updates = ", ".join(
        f"{column} = EXCLUDED.{column}"
        for column in METRIC_COLUMNS
        if column not in {"minute_start", "region"}
    )
    with _connect(settings) as connection:
        with connection.transaction():
            connection.execute(
                f"""
                INSERT INTO analytics.stream_metrics_minute ({columns})
                SELECT {columns}
                FROM analytics.stream_metrics_minute_staging
                WHERE batch_id = %s
                ON CONFLICT (minute_start, region) DO UPDATE SET {updates}
                """,
                (batch_id,),
            )
            connection.execute(
                "DELETE FROM analytics.stream_metrics_minute_staging WHERE batch_id = %s",
                (batch_id,),
            )
