import asyncio
from contextlib import closing
from uuid import uuid4

import httpx
import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from sqlalchemy import text
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
