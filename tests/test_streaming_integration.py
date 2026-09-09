import asyncio
import base64
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition

from pulseforge.generator import EventGenerator
from pulseforge.streaming.config import StreamSettings
from pulseforge.streaming.warehouse import connect
from tests.streaming_support import (
    checkpoint_ids,
    compose,
    event_count,
    eventually,
    lake_rows,
    pending_batch,
    send_records,
)
from tests.test_producer_integration import produce_real_events

pytestmark = [pytest.mark.integration, pytest.mark.streaming]


@pytest.fixture(scope="module")
def traffic():
    ids = asyncio.run(produce_real_events())
    event = EventGenerator(seed=uuid4().int).next_event(datetime.now(UTC) - timedelta(seconds=30))
    payload = event.model_dump_json().encode()
    missing = event.model_dump(mode="json")
    missing.pop("event_id")
    malformed = b'{"trace":"' + str(uuid4()).encode() + b'", broken\xff'
    invalid = json.dumps(missing).encode()
    sources = asyncio.run(send_records([payload, payload, malformed, invalid]))
    eventually(lambda: all(event_count(event_id) == 1 for event_id in [*ids, str(event.event_id)]))
    return {
        "ids": ids,
        "event": event,
        "payload": payload,
        "sources": sources,
        "malformed": malformed,
        "invalid": invalid,
    }


def test_actual_producer_reaches_raw_cleaned_curated_and_warehouse(traffic):
    expected = set(traffic["ids"])
    for layer in ("cleaned", "curated"):

        def all_expected_rows(layer=layer):
            result = [row for row in lake_rows(layer) if row["event_id"] in expected]
            return result if {row["event_id"] for row in result} == expected else None

        rows = eventually(all_expected_rows)
        assert {row["event_id"] for row in rows} == expected
        assert len(rows) == len(expected)
        assert all(row["source_topic"] == StreamSettings().kafka_topic for row in rows)
        if layer == "curated":
            assert all(row["event_date"] == row["event_ts"].date() for row in rows)
            assert all(
                row["is_payment_failure"] == (row["event_type"] == "payment_failed") for row in rows
            )
    raw = lake_rows("raw")
    raw_ids = set()
    for row in raw:
        try:
            payload = json.loads(row["raw_payload"])
        except (ValueError, TypeError):
            continue
        if isinstance(payload, dict) and isinstance(payload.get("event_id"), str):
            raw_ids.add(payload["event_id"])
    assert expected <= raw_ids


def test_duplicates_preserve_both_sources_but_one_logical_event(traffic):
    event_id = str(traffic["event"].event_id)
    expected_sources = set(traffic["sources"][:2])
    raw = lake_rows("raw")
    assert expected_sources <= {
        (row["source_topic"], row["source_partition"], row["source_offset"])
        for row in raw
        if row["raw_payload"] == traffic["payload"]
    }
    assert len([r for r in lake_rows("cleaned") if r["event_id"] == event_id]) == 1
    assert event_count(event_id) == 1


async def dlq_records(sources: set[tuple]) -> dict:
    settings = StreamSettings()
    consumer = AIOKafkaConsumer(
        settings.kafka_dlq_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_auto_commit=False,
        group_id=None,
    )
    await consumer.start()
    try:
        partitions = [
            TopicPartition(settings.kafka_dlq_topic, i)
            for i in consumer.partitions_for_topic(settings.kafka_dlq_topic)
        ]
        consumer.unsubscribe()
        consumer.assign(partitions)
        await consumer.seek_to_beginning(*partitions)
        found = {}
        async with asyncio.timeout(120):
            while found.keys() != sources:
                record = await consumer.getone()
                message = json.loads(record.value)
                source = (
                    message["source_topic"],
                    message["source_partition"],
                    message["source_offset"],
                )
                if source in sources:
                    assert record.key.decode() == ":".join(map(str, source))
                    found[source] = message
        return found
    finally:
        await consumer.stop()


def test_invalid_input_preserved_and_delivered_to_dlq(traffic):
    sources = traffic["sources"][2:]
    found = asyncio.run(dlq_records(set(sources)))
    assert found[sources[0]]["error_code"] == "malformed_json"
    assert found[sources[1]]["error_code"] == "missing_event_id"
    assert base64.b64decode(found[sources[0]]["payload_base64"]) == traffic["malformed"]
    raw = lake_rows("raw")
    assert all(
        any(row["raw_payload"] == traffic[name] for row in raw) for name in ("malformed", "invalid")
    )
    with connect(StreamSettings()) as connection:
        for source in sources:
            assert (
                connection.execute(
                    "SELECT count(*) FROM stream_events WHERE "
                    "(source_topic,source_partition,source_offset)=(%s,%s,%s)",
                    source,
                ).fetchone()[0]
                == 0
            )


def test_minute_aggregates_equal_unique_event_time_totals(traffic):
    with connect(StreamSettings()) as connection:
        discrepancies = connection.execute("""
            WITH expected AS (
                SELECT date_trunc('minute',event_ts) window_start,event_type,count(*) event_count,
                       coalesce(sum(amount),0) total_amount FROM stream_events GROUP BY 1,2
            ) SELECT count(*) FROM expected e FULL JOIN stream_metrics_minute m
              USING(window_start,event_type)
              WHERE e.event_count IS DISTINCT FROM m.event_count
                 OR e.total_amount IS DISTINCT FROM m.total_amount
        """).fetchone()[0]
    assert discrepancies == 0


def test_watermark_excludes_late_event_but_retains_raw_and_reports_drop(traffic):
    started = datetime.now(UTC).isoformat()
    old = EventGenerator(seed=uuid4().int).next_event(datetime.now(UTC) - timedelta(hours=1))
    sentinel = EventGenerator(seed=uuid4().int).next_event()
    payload = old.model_dump_json().encode()
    asyncio.run(send_records([payload, sentinel.model_dump_json().encode()]))
    eventually(lambda: event_count(str(sentinel.event_id)) == 1)
    assert event_count(str(old.event_id)) == 0
    assert any(row["raw_payload"] == payload for row in lake_rows("raw"))
    eventually(lambda: "watermark_dropped=1" in compose("logs", "--since", started, "streaming"))


def test_checkpoint_restart_retains_identity_and_dedup_state(traffic):
    before = checkpoint_ids()
    compose("stop", "streaming")
    event = EventGenerator(seed=uuid4().int).next_event()
    try:
        asyncio.run(send_records([traffic["payload"], event.model_dump_json().encode()]))
    finally:
        compose("up", "-d", "--wait", "--wait-timeout", "180", "streaming")
    eventually(lambda: event_count(str(event.event_id)) == 1)
    assert checkpoint_ids() == before
    assert event_count(str(traffic["event"].event_id)) == 1
    assert (
        len(
            [
                row
                for row in lake_rows("cleaned")
                if row["event_id"] == str(traffic["event"].event_id)
            ]
        )
        == 1
    )


def test_postgres_outage_fails_stream_and_recovers_uncommitted_input():
    settings = StreamSettings()
    before = checkpoint_ids()
    compose("stop", "postgres")
    event = EventGenerator(seed=uuid4().int).next_event()
    try:
        asyncio.run(send_records([event.model_dump_json().encode()]))
        eventually(
            lambda: '"State":"exited"' in compose("ps", "-a", "--format", "json", "streaming")
        )
    finally:
        compose("start", "postgres")
        eventually(lambda: "healthy" in compose("ps", "--format", "json", "postgres"))
    assert event_count(str(event.event_id)) == 0
    failed_batch = pending_batch()
    with connect(settings) as connection:
        assert (
            connection.execute(
                "SELECT count(*) FROM streaming_batches WHERE query_id=%s AND batch_id=%s",
                (before["valid-events"], failed_batch),
            ).fetchone()[0]
            == 0
        )
    compose("up", "-d", "--wait", "--wait-timeout", "180", "streaming")
    eventually(lambda: event_count(str(event.event_id)) == 1)
    assert checkpoint_ids() == before
