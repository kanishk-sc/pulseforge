import asyncio
from unittest.mock import AsyncMock

import pytest
from aiokafka.errors import KafkaConnectionError

from pulseforge.producer import run


@pytest.fixture
def producer(monkeypatch):
    producer = AsyncMock()
    monkeypatch.setattr("pulseforge.producer.AIOKafkaProducer", lambda **kwargs: producer)
    monkeypatch.setenv("EVENTS_PER_SECOND", "10000")
    return producer


async def test_finite_producer_confirms_each_record_and_closes(producer):
    assert await run(3, "normal") == 3
    assert producer.send_and_wait.await_count == 3
    producer.stop.assert_awaited_once()


async def test_delivery_failure_is_not_silently_retried(producer):
    producer.send_and_wait.side_effect = TimeoutError("ambiguous delivery")
    with pytest.raises(TimeoutError):
        await run(3, "normal")
    assert producer.send_and_wait.await_count == 1
    producer.stop.assert_awaited_once()


async def test_startup_retries_broker_unavailable(producer, monkeypatch):
    producer.start.side_effect = [KafkaConnectionError(), None]
    monkeypatch.setattr("pulseforge.producer.asyncio.sleep", AsyncMock())
    assert await run(1, "normal") == 1
    assert producer.start.await_count == 2


async def test_startup_retries_are_bounded(producer, monkeypatch):
    producer.start.side_effect = KafkaConnectionError()
    monkeypatch.setattr("pulseforge.producer.asyncio.sleep", AsyncMock())
    with pytest.raises(KafkaConnectionError):
        await run(1, "normal")
    assert producer.start.await_count == 5
    producer.stop.assert_awaited_once()


async def test_shutdown_stops_before_next_record(producer):
    stop = asyncio.Event()
    producer.send_and_wait.side_effect = lambda *args, **kwargs: stop.set()
    assert await run(0, "normal", stop=stop) == 1
    producer.stop.assert_awaited_once()
