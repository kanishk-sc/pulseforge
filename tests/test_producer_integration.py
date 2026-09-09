import asyncio
import os
import sys
from uuid import uuid4

import pytest
from aiokafka import AIOKafkaConsumer, TopicPartition

from pulseforge.config import Settings
from pulseforge.events import CommerceEvent
from pulseforge.generator import EventGenerator

pytestmark = pytest.mark.integration


async def produce_real_events(count: int = 8) -> list[str]:
    """Run the actual CLI with an isolated seed, consuming from captured end offsets."""
    settings = Settings()
    seed = uuid4().int % (2**31)
    generator = EventGenerator(seed=seed, anomaly_rate=0)
    expected = {
        str(CommerceEvent.model_validate_json(generator.next_record()[1]).event_id)
        for _ in range(count)
    }
    consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_auto_commit=False,
        group_id=None,
    )
    await consumer.start()
    try:
        partitions = [
            TopicPartition(settings.kafka_topic, i)
            for i in sorted(consumer.partitions_for_topic(settings.kafka_topic))
        ]
        consumer.unsubscribe()
        consumer.assign(partitions)
        await consumer.seek_to_end(*partitions)
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "pulseforge.producer",
            "--count",
            str(count),
            env={
                **os.environ,
                "GENERATOR_SEED": str(seed),
                "ANOMALY_RATE": "0",
                "EVENTS_PER_SECOND": "100",
            },
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr = await asyncio.wait_for(process.communicate(), timeout=60)
        except BaseException:
            process.kill()
            await process.wait()
            raise
        assert process.returncode == 0, stderr.decode()
        found = set()
        async with asyncio.timeout(30):
            while found != expected:
                record = await consumer.getone()
                try:
                    event = CommerceEvent.model_validate_json(record.value)
                except ValueError:
                    continue  # Other deliberately invalid demo traffic can coexist.
                if str(event.event_id) in expected:
                    found.add(str(event.event_id))
        return sorted(found)
    finally:
        await consumer.stop()


async def test_real_producer_cli_to_kafka_and_contract():
    assert len(await produce_real_events()) == 8
