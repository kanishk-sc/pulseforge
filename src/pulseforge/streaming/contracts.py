from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)

ALLOWED_FIELDS = (
    "schema_version",
    "event_id",
    "event_type",
    "timestamp",
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
)

EVENT_TYPES = (
    "order_created",
    "payment_processed",
    "payment_failed",
    "shipment_created",
    "shipment_delayed",
    "refund_requested",
    "inventory_updated",
    "customer_login",
)

REGIONS = ("us-east", "us-west", "eu-west", "ap-south")
PAYMENT_PROVIDERS = ("stripe", "adyen", "paypal")
SHIPMENT_PROVIDERS = ("ups", "fedex", "dhl")
STATUS_BY_TYPE = {
    "order_created": "created",
    "payment_processed": "paid",
    "payment_failed": "failed",
    "shipment_created": "shipped",
    "shipment_delayed": "delayed",
    "refund_requested": "requested",
    "inventory_updated": "updated",
    "customer_login": "authenticated",
}

# This mirrors CommerceEvent in pulseforge.events. String timestamps are parsed explicitly so
# invalid timestamps remain classifiable instead of turning the whole record into null.
EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", IntegerType(), True),
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DecimalType(14, 2), True),
        StructField("currency", StringType(), True),
        StructField("payment_provider", StringType(), True),
        StructField("shipment_provider", StringType(), True),
        StructField("region", StringType(), True),
        StructField("status", StringType(), True),
        StructField("metadata", MapType(StringType(), StringType()), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)
