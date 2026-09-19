import os
from dataclasses import dataclass


def _required(name: str) -> str:
    value = os.getenv(name, "")
    if not value:
        raise ValueError(f"{name} must be set")
    return value


@dataclass(frozen=True)
class StreamingSettings:
    kafka_bootstrap_servers: str
    kafka_topic: str
    kafka_dead_letter_topic: str
    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_password: str
    postgres_db: str
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    s3_bucket: str
    checkpoint_root: str
    watermark_delay: str
    trigger_interval: str
    max_offsets_per_trigger: int
    shuffle_partitions: int

    @classmethod
    def from_env(cls) -> "StreamingSettings":
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            kafka_topic=os.getenv("KAFKA_TOPIC", "commerce.events.v1"),
            kafka_dead_letter_topic=os.getenv("KAFKA_DEAD_LETTER_TOPIC", "commerce.dead-letter.v1"),
            postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_user=os.getenv("POSTGRES_USER", "pulseforge"),
            postgres_password=_required("POSTGRES_PASSWORD"),
            postgres_db=os.getenv("POSTGRES_DB", "pulseforge"),
            s3_endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://minio:9000"),
            s3_access_key=os.getenv("MINIO_ROOT_USER", "pulseforge"),
            s3_secret_key=_required("MINIO_ROOT_PASSWORD"),
            s3_bucket=os.getenv("S3_BUCKET", "pulseforge"),
            checkpoint_root=os.getenv("SPARK_CHECKPOINT_ROOT", "/opt/spark/checkpoints"),
            watermark_delay=os.getenv("SPARK_WATERMARK_DELAY", "10 minutes"),
            trigger_interval=os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds"),
            max_offsets_per_trigger=int(os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "10000")),
            shuffle_partitions=int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4")),
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    @property
    def lake_events_uri(self) -> str:
        return f"s3a://{self.s3_bucket}/cleaned/stream_events"

    @property
    def lake_raw_uri(self) -> str:
        return f"s3a://{self.s3_bucket}/raw/stream_events"

    def checkpoint(self, query_name: str) -> str:
        return f"{self.checkpoint_root.rstrip('/')}/{query_name}"
