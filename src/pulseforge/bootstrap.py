"""Idempotent local initialization; no destructive changes or implicit migrations."""

import asyncio
import logging
from contextlib import closing

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from botocore.exceptions import ClientError

from pulseforge.config import Settings
from pulseforge.dependencies import s3_client
from pulseforge.logging import configure_logging

logger = logging.getLogger(__name__)


async def initialize() -> None:
    settings = Settings()
    client = AIOKafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap_servers)
    try:
        await client.start()
        for name in (settings.kafka_topic, "commerce.dead-letter.v1"):
            try:
                await client.create_topics(
                    [
                        NewTopic(
                            name,
                            num_partitions=3,
                            replication_factor=1,
                            topic_configs={"retention.ms": "604800000"},
                        )
                    ]
                )
            except TopicAlreadyExistsError:
                pass
            logger.info("topic_ready %s", name)
    finally:
        await client.close()
    with closing(s3_client(settings)) as storage:
        try:
            storage.head_bucket(Bucket=settings.s3_bucket)
        except ClientError as exc:
            if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
                raise
            storage.create_bucket(Bucket=settings.s3_bucket)
        logger.info("bucket_ready %s", settings.s3_bucket)


if __name__ == "__main__":
    configure_logging()
    asyncio.run(initialize())
