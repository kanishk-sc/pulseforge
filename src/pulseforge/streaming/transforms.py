from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pulseforge.streaming.contracts import (
    ALLOWED_FIELDS,
    EVENT_SCHEMA,
    EVENT_TYPES,
    PAYMENT_PROVIDERS,
    REGIONS,
    SHIPMENT_PROVIDERS,
    STATUS_BY_TYPE,
)

UUID_PATTERN = (
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
)
IDENTIFIER_PATTERN = r"^\S{1,100}$"
MONEY_PATTERN = r"^(0|[1-9][0-9]{0,11})(\.[0-9]{1,2})?$"
NONNEGATIVE_INTEGER_PATTERN = r"^(0|[1-9][0-9]*)$"


def _one_of(column: F.Column, values: tuple[str, ...]) -> F.Column:
    return column.isin(*values)


def _missing_identifier(name: str) -> F.Column:
    return F.col(name).isNull() | ~F.col(name).rlike(IDENTIFIER_PATTERN)


def classify_events(kafka_frame: DataFrame) -> DataFrame:
    """Parse Kafka records and attach stable validation codes without dropping evidence."""
    base = kafka_frame.select(
        F.col("topic").alias("source_topic"),
        F.col("partition").alias("source_partition"),
        F.col("offset").alias("source_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").alias("raw_bytes"),
        F.col("value").cast("string").alias("raw_value"),
    )
    parsed = (
        base.withColumn(
            "event",
            F.from_json(
                "raw_value",
                EVENT_SCHEMA,
                {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
            ),
        )
        .withColumn("event_timestamp", F.to_timestamp("event.timestamp"))
        .withColumn("source_event_id", F.get_json_object("raw_value", "$.event_id"))
        .withColumn("amount_text", F.get_json_object("raw_value", "$.amount"))
        .withColumn(
            "inventory_quantity_text",
            F.get_json_object("raw_value", "$.metadata.quantity"),
        )
        .withColumn("json_fields", F.expr("json_object_keys(raw_value)"))
        .withColumn("allowed_fields", F.array(*[F.lit(name) for name in ALLOWED_FIELDS]))
    )
    flattened = parsed.select(
        "source_topic",
        "source_partition",
        "source_offset",
        "kafka_timestamp",
        "kafka_key",
        "raw_bytes",
        "raw_value",
        "source_event_id",
        "amount_text",
        "inventory_quantity_text",
        "json_fields",
        "allowed_fields",
        "event_timestamp",
        F.col("event").isNull().alias("parse_failed"),
        F.col("event._corrupt_record").alias("corrupt_record"),
        *[F.col(f"event.{name}").alias(name) for name in ALLOWED_FIELDS],
    )

    status_is_valid = reduce(
        lambda left, right: left | right,
        [
            (F.col("event_type") == event_type) & (F.col("status") == status)
            for event_type, status in STATUS_BY_TYPE.items()
        ],
    )
    money_event = F.col("event_type").isin(
        "order_created", "payment_processed", "payment_failed", "refund_requested"
    )
    payment_event = F.col("event_type").isin("payment_processed", "payment_failed")
    shipment_event = F.col("event_type").isin("shipment_created", "shipment_delayed")
    order_event = ~F.col("event_type").isin("inventory_updated", "customer_login")
    customer_event = F.col("event_type") != "inventory_updated"

    checks = [
        (F.col("corrupt_record").isNotNull() | F.col("parse_failed"), "malformed_json"),
        (F.size(F.array_except("json_fields", "allowed_fields")) > 0, "unknown_field"),
        (F.col("schema_version") != 1, "unsupported_schema_version"),
        (
            F.col("event_id").isNull() | ~F.col("event_id").rlike(UUID_PATTERN),
            "invalid_event_id",
        ),
        (~_one_of(F.col("event_type"), EVENT_TYPES), "invalid_event_type"),
        (
            F.col("event_timestamp").isNull()
            | (F.col("event_timestamp") < F.to_timestamp(F.lit("2020-01-01T00:00:00Z")))
            | (F.col("event_timestamp") > F.current_timestamp() + F.expr("INTERVAL 5 MINUTES")),
            "invalid_timestamp",
        ),
        (~_one_of(F.col("region"), REGIONS), "invalid_region"),
        (F.col("currency") != "USD", "invalid_currency"),
        (~status_is_valid, "invalid_status"),
        (customer_event & _missing_identifier("customer_id"), "invalid_customer_id"),
        (order_event & _missing_identifier("order_id"), "invalid_order_id"),
        (
            (F.col("event_type") != "customer_login") & _missing_identifier("product_id"),
            "invalid_product_id",
        ),
        (
            money_event & (F.col("amount").isNull() | ~F.col("amount_text").rlike(MONEY_PATTERN)),
            "invalid_amount",
        ),
        (
            payment_event & ~_one_of(F.col("payment_provider"), PAYMENT_PROVIDERS),
            "invalid_payment_provider",
        ),
        (
            shipment_event & ~_one_of(F.col("shipment_provider"), SHIPMENT_PROVIDERS),
            "invalid_shipment_provider",
        ),
        (
            (F.col("event_type") == "inventory_updated")
            & ~F.col("inventory_quantity_text").rlike(NONNEGATIVE_INTEGER_PATTERN),
            "invalid_inventory_quantity",
        ),
    ]
    return (
        flattened.withColumn(
            "validation_errors",
            F.array_compact(
                F.array(*[F.when(condition, F.lit(code)) for condition, code in checks])
            ),
        )
        .withColumn("is_valid", F.size("validation_errors") == 0)
        .withColumn("processed_at", F.current_timestamp())
        .withColumn(
            "processing_latency_ms",
            F.greatest(
                F.lit(0),
                (F.col("processed_at").cast("double") - F.col("event_timestamp").cast("double"))
                * F.lit(1000),
            ),
        )
        .drop(
            "parse_failed",
            "corrupt_record",
            "amount_text",
            "inventory_quantity_text",
            "json_fields",
            "allowed_fields",
        )
    )


def valid_events(classified: DataFrame, watermark_delay: str) -> DataFrame:
    events = classified.filter("is_valid").withWatermark("event_timestamp", watermark_delay)
    if not events.isStreaming:
        return events.dropDuplicates(["event_id"])
    if hasattr(events, "dropDuplicatesWithinWatermark"):
        return events.dropDuplicatesWithinWatermark(["event_id"])
    return events.dropDuplicates(["event_id"])


def dead_letter_records(classified: DataFrame) -> DataFrame:
    invalid = classified.filter("NOT is_valid")
    fallback_key = F.concat_ws(
        ":", "source_topic", F.col("source_partition"), F.col("source_offset")
    )
    return invalid.select(
        F.coalesce("source_event_id", fallback_key).cast("string").alias("key"),
        F.to_json(
            F.struct(
                F.lit("pulseforge.spark.v1").alias("rejection_schema"),
                "source_event_id",
                "validation_errors",
                F.base64("raw_bytes").alias("raw_payload_base64"),
                "raw_value",
                F.struct(
                    "source_topic",
                    "source_partition",
                    "source_offset",
                    "kafka_timestamp",
                ).alias("source"),
                "processed_at",
            )
        ).alias("value"),
    )


def raw_archive_rows(classified: DataFrame) -> DataFrame:
    """Preserve every consumed Kafka value losslessly before validity filtering."""
    return classified.select(
        "source_topic",
        "source_partition",
        "source_offset",
        "kafka_timestamp",
        "kafka_key",
        "raw_bytes",
        "raw_value",
        "source_event_id",
        "validation_errors",
        "is_valid",
        "processed_at",
        F.to_date("kafka_timestamp").alias("ingest_date"),
    )


def event_sink_rows(events: DataFrame) -> DataFrame:
    return events.select(
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
        F.to_json("metadata").alias("metadata_json"),
        "source_topic",
        "source_partition",
        "source_offset",
        "kafka_timestamp",
        "processed_at",
        "processing_latency_ms",
        "raw_value",
    )


def archived_events(events: DataFrame) -> DataFrame:
    return event_sink_rows(events).withColumn("event_date", F.to_date("event_timestamp"))


def minute_metrics(events: DataFrame) -> DataFrame:
    payment_attempt = F.col("event_type").isin("payment_processed", "payment_failed")
    aggregates = events.groupBy(F.window("event_timestamp", "1 minute"), "region").agg(
        F.count("*").alias("event_count"),
        F.sum(F.when(F.col("event_type") == "order_created", 1).otherwise(0)).alias("order_count"),
        F.sum(F.when(F.col("event_type") == "payment_processed", 1).otherwise(0)).alias(
            "successful_payment_count"
        ),
        F.sum(F.when(F.col("event_type") == "payment_failed", 1).otherwise(0)).alias(
            "failed_payment_count"
        ),
        F.sum(F.when(payment_attempt, 1).otherwise(0)).alias("payment_attempt_count"),
        F.sum(
            F.when(F.col("event_type") == "payment_processed", F.col("amount")).otherwise(F.lit(0))
        ).alias("successful_revenue"),
        F.sum(F.when(F.col("event_type") == "shipment_created", 1).otherwise(0)).alias(
            "shipment_created_count"
        ),
        F.sum(F.when(F.col("event_type") == "shipment_delayed", 1).otherwise(0)).alias(
            "shipment_delayed_count"
        ),
        F.sum(F.when(F.col("event_type") == "refund_requested", 1).otherwise(0)).alias(
            "refund_request_count"
        ),
        F.avg("processing_latency_ms").alias("avg_processing_latency_ms"),
        F.max("processed_at").alias("updated_at"),
    )
    return aggregates.select(
        F.col("window.start").alias("minute_start"),
        F.col("window.end").alias("minute_end"),
        "region",
        "event_count",
        "order_count",
        "successful_payment_count",
        "failed_payment_count",
        "payment_attempt_count",
        F.coalesce("successful_revenue", F.lit(0)).alias("successful_revenue"),
        F.when(
            F.col("payment_attempt_count") > 0,
            F.col("failed_payment_count") / F.col("payment_attempt_count"),
        )
        .otherwise(F.lit(0.0))
        .alias("payment_failure_rate"),
        "shipment_created_count",
        "shipment_delayed_count",
        "refund_request_count",
        "avg_processing_latency_ms",
        "updated_at",
    )
