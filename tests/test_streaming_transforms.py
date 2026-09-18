import gc
import json
import logging
import os
import sys
from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from pulseforge.generator import EventGenerator
from pulseforge.streaming.transforms import (
    classify_events,
    dead_letter_records,
    minute_metrics,
    raw_archive_rows,
    valid_events,
)

pytestmark = pytest.mark.spark

KAFKA_SCHEMA = T.StructType(
    [
        T.StructField("topic", T.StringType(), False),
        T.StructField("partition", T.IntegerType(), False),
        T.StructField("offset", T.LongType(), False),
        T.StructField("timestamp", T.TimestampType(), False),
        T.StructField("key", T.BinaryType(), True),
        T.StructField("value", T.BinaryType(), False),
    ]
)


@pytest.fixture(scope="module")
def spark():
    # Spark's Windows launcher cannot quote a Python path below this workspace's directory
    # because it contains a space. The uv-managed base interpreter has the same version and
    # a space-free path; PySpark supplies its worker modules through PYTHONPATH.
    worker_python = sys._base_executable
    os.environ["PYSPARK_PYTHON"] = worker_python
    session = (
        SparkSession.builder.master("local[1]")
        .appName("pulseforge-transform-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", worker_python)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
    # Py4J owns a cyclic client/server reference; collect it while pytest's capture stream
    # is still open so its finalizer cannot emit a logging traceback at interpreter exit.
    logging.getLogger("py4j.clientserver").disabled = True
    gc.collect()


def kafka_frame(spark, payloads: list[bytes]):
    now = datetime.now(UTC).replace(tzinfo=None)
    rows = [
        ("commerce.events.v1", 0, offset, now, b"key", value)
        for offset, value in enumerate(payloads)
    ]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def valid_payload(kind: str | None = None) -> bytes:
    generator = EventGenerator(anomaly_rate=0)
    while True:
        event = generator.next_event()
        if kind is None or event.event_type == kind:
            return event.model_dump_json().encode()


def test_classifies_valid_event_and_preserves_source_metadata(spark):
    payload = valid_payload("payment_processed")
    row = classify_events(kafka_frame(spark, [payload])).first()
    assert row.is_valid is True
    assert row.validation_errors == []
    assert row.event_id == json.loads(payload)["event_id"]
    assert (row.source_topic, row.source_partition, row.source_offset) == (
        "commerce.events.v1",
        0,
        0,
    )
    assert bytes(row.raw_bytes) == payload


@pytest.mark.parametrize(
    "payload,expected",
    [
        (b'{"schema_version":1,"broken":', "malformed_json"),
        (
            lambda: json.dumps({**json.loads(valid_payload()), "event_id": None}).encode(),
            "invalid_event_id",
        ),
        (
            lambda: json.dumps({**json.loads(valid_payload()), "unexpected": "field"}).encode(),
            "unknown_field",
        ),
    ],
)
def test_rejects_invalid_payload_with_reason(spark, payload, expected):
    value = payload() if callable(payload) else payload
    classified = classify_events(kafka_frame(spark, [value]))
    row = classified.first()
    assert row.is_valid is False
    assert expected in row.validation_errors
    dead_letter = json.loads(dead_letter_records(classified).first().value)
    assert expected in dead_letter["validation_errors"]
    assert dead_letter["raw_value"] == value.decode()
    assert dead_letter["raw_payload_base64"]


def test_raw_archive_preserves_non_utf8_bytes(spark):
    payload = b"\xff\xfe\x00not-json"
    archived = raw_archive_rows(classify_events(kafka_frame(spark, [payload]))).first()
    assert bytes(archived.raw_bytes) == payload
    assert archived.is_valid is False
    assert "malformed_json" in archived.validation_errors


def test_deduplicates_event_ids_within_watermark(spark):
    payload = valid_payload()
    classified = classify_events(kafka_frame(spark, [payload, payload]))
    assert valid_events(classified, "10 minutes").count() == 1


def test_minute_metrics_use_payment_attempt_denominator_and_successful_revenue(spark):
    generator = EventGenerator(anomaly_rate=0)
    while True:
        event = generator.next_event()
        if event.event_type.value == "payment_processed":
            success = event.model_dump(mode="json")
            break
    failed = {**success, "event_type": "payment_failed", "status": "failed"}
    failed["event_id"] = str(EventGenerator(seed=100).next_event().event_id)
    payloads = [json.dumps(success).encode(), json.dumps(failed).encode()]
    accepted = valid_events(classify_events(kafka_frame(spark, payloads)), "10 minutes")
    rows = minute_metrics(accepted).collect()
    assert sum(row.payment_attempt_count for row in rows) == 2
    assert sum(row.failed_payment_count for row in rows) == 1
    assert sum(float(row.successful_revenue) for row in rows) > 0
    assert {row.payment_failure_rate for row in rows} == {0.5}
