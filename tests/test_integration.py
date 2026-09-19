import asyncio
import json
from contextlib import closing
from uuid import uuid4

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from pulseforge.bootstrap import initialize
from pulseforge.config import Settings
from pulseforge.dependencies import s3_client
from pulseforge.events import CommerceEvent
from pulseforge.generator import EventGenerator

pytestmark = pytest.mark.integration


async def test_live_api_readiness():
    async with httpx.AsyncClient() as client:
        response = await client.get("http://localhost:8000/ready", timeout=10)
    assert response.status_code == 200
    assert set(response.json()["dependencies"].values()) == {"up"}


async def test_bootstrap_is_idempotent():
    await initialize()
    await initialize()


async def test_postgres_transaction():
    engine = create_async_engine(Settings().database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("CREATE TEMP TABLE smoke (id integer PRIMARY KEY)"))
            await connection.execute(text("INSERT INTO smoke VALUES (1)"))
            assert (await connection.execute(text("SELECT count(*) FROM smoke"))).scalar() == 1
    finally:
        await engine.dispose()


def test_s3_object_roundtrip():
    settings = Settings()
    key = f"smoke/{uuid4()}.json"
    with closing(s3_client(settings)) as storage:
        try:
            storage.put_object(Bucket=settings.s3_bucket, Key=key, Body=b'{"synthetic":true}')
            response = storage.get_object(Bucket=settings.s3_bucket, Key=key)
            with response["Body"] as body:
                assert body.read() == b'{"synthetic":true}'
        finally:
            storage.delete_object(Bucket=settings.s3_bucket, Key=key)


async def test_kafka_delivery_and_readback():
    settings = Settings()
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers, enable_idempotence=True
    )
    consumer = AIOKafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_auto_commit=False,
        group_id=None,
    )
    await producer.start()
    await consumer.start()
    try:
        event = EventGenerator(seed=uuid4().int).next_event()
        metadata = await asyncio.wait_for(
            producer.send_and_wait(
                settings.kafka_topic, value=event.model_dump_json().encode(), key=b"integration"
            ),
            timeout=15,
        )
        partition = TopicPartition(metadata.topic, metadata.partition)
        consumer.assign([partition])
        consumer.seek(partition, metadata.offset)
        record = await asyncio.wait_for(consumer.getone(), timeout=15)
        assert CommerceEvent.model_validate_json(record.value) == event
    finally:
        await consumer.stop()
        await producer.stop()


async def test_streaming_pipeline_to_all_sinks():
    settings = Settings()
    token = f"stream-smoke-{uuid4()}"
    event = EventGenerator(seed=uuid4().int).next_event()
    malformed = event.model_dump(mode="json")
    malformed.pop("event_id")
    malformed["metadata"] = {**malformed["metadata"], "integration_token": token}

    with closing(s3_client(settings)) as storage:
        before = {
            prefix: sum(
                item["Size"]
                for item in storage.list_objects_v2(Bucket=settings.s3_bucket, Prefix=prefix).get(
                    "Contents", []
                )
            )
            for prefix in ("raw/stream_events/", "cleaned/stream_events/")
        }

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers, enable_idempotence=True
    )
    rejected = AIOKafkaConsumer(
        settings.kafka_dead_letter_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_auto_commit=False,
        auto_offset_reset="latest",
        group_id=None,
    )
    await rejected.start()
    await producer.start()
    engine = create_async_engine(settings.database_url)
    try:
        # Force partition assignment before publishing so "latest" cannot skip this rejection.
        await rejected.getmany(timeout_ms=1000)
        await producer.send_and_wait(
            settings.kafka_topic,
            value=event.model_dump_json().encode(),
            key=b"stream-integration-valid",
        )
        await producer.send_and_wait(
            settings.kafka_topic,
            value=json.dumps(malformed).encode(),
            key=b"stream-integration-invalid",
        )

        async def warehouse_has_event() -> bool:
            try:
                async with engine.connect() as connection:
                    result = await connection.execute(
                        text(
                            "SELECT count(*) FROM analytics.stream_events "
                            "WHERE event_id = CAST(:event_id AS uuid)"
                        ),
                        {"event_id": str(event.event_id)},
                    )
                return result.scalar_one() == 1
            except DBAPIError:
                return False

        for _ in range(120):
            if await warehouse_has_event():
                break
            await asyncio.sleep(1)
        else:
            pytest.fail("valid event did not reach the PostgreSQL stream sink")

        for _ in range(120):
            try:
                record = await asyncio.wait_for(rejected.getone(), timeout=1)
            except TimeoutError:
                continue
            envelope = json.loads(record.value)
            if token in envelope["raw_value"]:
                assert "invalid_event_id" in envelope["validation_errors"]
                assert envelope["raw_payload_base64"]
                break
        else:
            pytest.fail("invalid event did not reach the dead-letter topic")

        for _ in range(120):
            with closing(s3_client(settings)) as storage:
                after = {
                    prefix: sum(
                        item["Size"]
                        for item in storage.list_objects_v2(
                            Bucket=settings.s3_bucket, Prefix=prefix
                        ).get("Contents", [])
                    )
                    for prefix in before
                }
            if all(after[prefix] > before[prefix] for prefix in before):
                break
            await asyncio.sleep(1)
        else:
            pytest.fail("raw and cleaned lake prefixes did not both grow")
    finally:
        await engine.dispose()
        await rejected.stop()
        await producer.stop()
