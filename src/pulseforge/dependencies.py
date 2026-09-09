import asyncio
import logging
from contextlib import closing

import boto3
from aiokafka.admin import AIOKafkaAdminClient
from botocore.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from pulseforge.config import Settings

logger = logging.getLogger(__name__)


def s3_client(settings: Settings):
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
        aws_access_key_id=settings.minio_root_user,
        aws_secret_access_key=settings.minio_root_password.get_secret_value(),
        config=Config(connect_timeout=2, read_timeout=2, retries={"max_attempts": 0}),
    )


async def check_dependencies(settings: Settings, engine: AsyncEngine) -> dict[str, str]:
    async def postgres() -> None:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def kafka() -> None:
        client = AIOKafkaAdminClient(
            bootstrap_servers=settings.kafka_bootstrap_servers, request_timeout_ms=2000
        )
        try:
            await client.start()
            topics = await client.list_topics()
            if settings.kafka_topic not in topics:
                raise RuntimeError("required topic is missing")
        finally:
            await client.close()

    async def storage() -> None:
        def probe() -> None:
            with closing(s3_client(settings)) as client:
                client.head_bucket(Bucket=settings.s3_bucket)

        await asyncio.to_thread(probe)

    async def check(name: str, operation) -> tuple[str, str]:
        try:
            await asyncio.wait_for(operation(), timeout=4)
            return name, "up"
        except Exception as exc:
            logger.warning(
                "dependency_unavailable %s", name, extra={"error_type": type(exc).__name__}
            )
            return name, "down"

    return dict(
        await asyncio.gather(
            check("postgres", postgres), check("kafka", kafka), check("object_storage", storage)
        )
    )
