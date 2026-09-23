"""Driver-side diagnostic metrics; no executor task directly increments counters."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

QUERY_NAMES = ("raw", "dlq", "valid-events", "validation-counts")

LAST_PROGRESS = Gauge(
    "pulseforge_stream_last_progress_timestamp_seconds",
    "Driver time of the latest successful Spark progress callback",
    ("query",),
)
INPUT_ROWS = Counter(
    "pulseforge_stream_input_rows_total",
    "Spark input rows in completed batch attempts, not unique warehouse events",
    ("query",),
)
PROCESSED_RATE = Gauge(
    "pulseforge_stream_processed_rows_per_second",
    "Spark-reported processed rows per second in the latest batch",
    ("query",),
)
BATCH_DURATION = Histogram(
    "pulseforge_stream_batch_duration_seconds",
    "Spark-reported trigger duration",
    ("query",),
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, float("inf")),
)
WATERMARK_DROPS = Counter(
    "pulseforge_stream_watermark_drops_total",
    "State-operator watermark drops in completed batch attempts",
    ("query",),
)
SOURCE_OFFSET = Gauge(
    "pulseforge_stream_source_offset",
    "Raw query ending Kafka offset reported by Spark; not consumer-group lag",
    ("partition",),
)
VALIDATION_REJECTIONS = Counter(
    "pulseforge_stream_validation_rejection_attempts_total",
    "Validation rejection rows in successful diagnostic batch attempts",
    ("reason",),
)
SINK_INPUT_ROWS = Counter(
    "pulseforge_stream_sink_input_attempt_rows_total",
    "Rows presented to successful sink callback attempts",
)
SINK_INSERTED_ROWS = Counter(
    "pulseforge_stream_sink_inserted_attempt_rows_total",
    "Inserted rows reported by successful sink callback attempts; ledger is authoritative",
)
SINK_FAILURES = Counter(
    "pulseforge_stream_sink_failures_total",
    "Failed sink callback attempts",
    ("dependency",),
)
QUERY_FAILURES = Counter(
    "pulseforge_stream_query_failures_total",
    "Streaming query terminations with an exception, including optional diagnostics",
    ("query",),
)


def start_metrics_server() -> None:
    start_http_server(9109, addr="0.0.0.0")
