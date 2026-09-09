from pydantic import Field

from pulseforge.config import Settings


class StreamSettings(Settings):
    # Compose exposes IPv4 loopback; avoid Windows IPv6 localhost connection fallback delays.
    postgres_host: str = "127.0.0.1"
    stream_namespace: str = Field(default="v1", pattern=r"^[a-z0-9_-]{1,40}$")
    stream_watermark: str = "10 minutes"
    stream_trigger: str = "5 seconds"
    stream_max_offsets: int = Field(default=1000, ge=1, le=100000)
    stream_shuffle_partitions: int = Field(default=2, ge=1)
    kafka_dlq_topic: str = "commerce.dead-letter.v1"

    def path(self, layer: str) -> str:
        return f"s3a://{self.s3_bucket}/{layer}/{self.stream_namespace}"

    def checkpoint(self, query: str) -> str:
        return self.path(f"checkpoints/{query}")

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
            "?connectTimeout=5&socketTimeout=30&options=-c%20TimeZone=UTC"
        )
