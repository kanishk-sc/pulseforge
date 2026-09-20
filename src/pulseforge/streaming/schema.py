from pyspark.sql.types import (
    BinaryType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RAW_SCHEMA = StructType(
    [
        StructField("source_topic", StringType()),
        StructField("source_partition", IntegerType()),
        StructField("source_offset", LongType()),
        StructField("source_timestamp", TimestampType()),
        StructField("key", BinaryType()),
        StructField("raw_payload", BinaryType()),
        StructField("ingested_at", TimestampType()),
        StructField("ingest_date", DateType()),
    ]
)

EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType()),
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("customer_id", StringType()),
        StructField("order_id", StringType()),
        StructField("product_id", StringType()),
        StructField("amount", DecimalType(14, 2)),
        StructField("currency", StringType()),
        StructField("payment_provider", StringType()),
        StructField("shipment_provider", StringType()),
        StructField("region", StringType()),
        StructField("status", StringType()),
        StructField("metadata", StringType()),
    ]
)

VALIDATION_SCHEMA = StructType(
    [
        StructField("event", EVENT_SCHEMA),
        StructField("error_code", StringType()),
        StructField("error_detail", StringType()),
    ]
)
