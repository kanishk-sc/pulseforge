import asyncio
import json
import subprocess
import time
from contextlib import closing

from aiokafka import AIOKafkaProducer

from pulseforge.dependencies import s3_client
from pulseforge.streaming.config import StreamSettings
from pulseforge.streaming.inspect import object_keys, read_bytes
from pulseforge.streaming.inspect import read_layer as lake_rows  # noqa: F401
from pulseforge.streaming.warehouse import connect


def compose(*args: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "--profile", "streaming", *args],
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout


def eventually(check, timeout: int = 180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        time.sleep(2)
    raise AssertionError(f"Condition did not become true in {timeout}s")


def event_count(event_id: str) -> int:
    with connect(StreamSettings()) as connection:
        return connection.execute(
            "SELECT count(*) FROM stream_events WHERE event_id=%s", (event_id,)
        ).fetchone()[0]


async def send_records(payloads: list[bytes]) -> list[tuple[str, int, int]]:
    settings = StreamSettings()
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    await producer.start()
    try:
        result = []
        for payload in payloads:
            metadata = await asyncio.wait_for(
                producer.send_and_wait(
                    settings.kafka_topic, value=payload, key=b"streaming-test", partition=0
                ),
                timeout=20,
            )
            result.append((metadata.topic, metadata.partition, metadata.offset))
        return result
    finally:
        await producer.stop()


def checkpoint_ids() -> dict[str, str]:
    settings = StreamSettings()
    with closing(s3_client(settings)) as storage:
        return {
            query: json.loads(
                read_bytes(
                    storage,
                    settings.s3_bucket,
                    f"checkpoints/{query}/{settings.stream_namespace}/metadata",
                )
            )["id"]
            for query in ("raw", "dlq", "valid-events")
        }


def pending_batch() -> int:
    settings = StreamSettings()
    with closing(s3_client(settings)) as storage:
        prefix = f"checkpoints/valid-events/{settings.stream_namespace}"
        offsets = {
            int(key.rsplit("/", 1)[-1])
            for key in object_keys(storage, settings.s3_bucket, prefix + "/offsets/")
            if key.rsplit("/", 1)[-1].isdigit()
        }
        commits = {
            int(key.rsplit("/", 1)[-1])
            for key in object_keys(storage, settings.s3_bucket, prefix + "/commits/")
            if key.rsplit("/", 1)[-1].isdigit()
        }
        pending = offsets - commits
        assert pending, "Failure must leave an offset batch without a Spark commit"
        return max(pending)
