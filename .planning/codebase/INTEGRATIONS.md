# External Integrations

**Analysis Date:** 2026-09-18

## Kafka

- `src/pulseforge/bootstrap.py` creates `commerce.events.v1` and `commerce.dead-letter.v1` with three partitions.
- `src/pulseforge/producer.py` publishes keyed records with producer idempotence and acknowledged delivery.
- `src/pulseforge/dependencies.py` verifies topic presence for API readiness.
- `tests/test_integration.py` performs a real broker write/read round trip.
- The dead-letter topic exists but no consumer routes invalid records to it yet.

## PostgreSQL

- `src/pulseforge/config.py` constructs a credential-safe SQLAlchemy URL.
- The API creates an async engine and readiness performs `SELECT 1`.
- Compose provides PostgreSQL 16 with persistent storage and a health check.
- No migrations, schemas, tables, warehouse loads, or analytics queries exist yet.

## S3-compatible storage

- MinIO is the local S3-compatible implementation in `docker-compose.yml`.
- Bootstrap creates the configured bucket idempotently.
- Readiness verifies bucket access without returning raw connection errors.
- The integration suite covers put, get, and delete behavior.
- No event archive or analytical dataset is written to object storage yet.

## HTTP API

- FastAPI exposes `/health`, `/ready`, and OpenAPI documentation.
- Request IDs are accepted only when they match a bounded safe pattern.
- There are no analytics, metrics, authentication, or mutation endpoints.

## CI and container registry

- GitHub Actions uses official checkout and setup-uv actions.
- The workflow builds local images but does not publish to a registry.
- CI does not need cloud credentials or paid APIs.

## Absent integrations

- There is no Redis connection, Spark connector, dbt adapter, Airflow provider, telemetry exporter, or AWS API integration.
- No external LLM provider is configured or claimed as implemented.

*Integration analysis: 2026-09-18*
