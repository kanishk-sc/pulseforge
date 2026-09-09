from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import URL


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "pulseforge"
    postgres_db: str = "pulseforge"
    postgres_password: SecretStr = SecretStr("")
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "commerce.events.v1"
    s3_endpoint_url: str = "http://localhost:9000"
    minio_root_user: str = "pulseforge"
    minio_root_password: SecretStr = SecretStr("")
    s3_region: str = "us-east-1"
    s3_bucket: str = "pulseforge"
    events_per_second: float = Field(default=10, gt=0, le=10000)
    anomaly_rate: float = Field(default=0.02, ge=0, le=1)
    generator_seed: int = 42

    @property
    def database_url(self) -> URL:
        return URL.create(
            "postgresql+asyncpg",
            username=self.postgres_user,
            password=self.postgres_password.get_secret_value(),
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        )
