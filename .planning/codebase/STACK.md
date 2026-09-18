# Technology Stack

**Analysis Date:** 2026-09-18

## Runtime and languages

- Python 3.12 is the only application language currently implemented.
- `uv` 0.11.16 manages the locked Python environment in `uv.lock`.
- The package uses a `src/` layout rooted at `src/pulseforge`.
- JSON Schema is generated from the Pydantic event contract.
- YAML defines Docker Compose and GitHub Actions automation.

## Application frameworks

- FastAPI exposes liveness and dependency-readiness endpoints in `src/pulseforge/api.py`.
- Pydantic 2 defines the event contract and environment settings.
- SQLAlchemy async and `asyncpg` currently support PostgreSQL health checks only.
- `aiokafka` implements the producer, topic bootstrap, and integration tests.
- `boto3` implements MinIO/S3 bootstrap, readiness, and object round-trip tests.

## Infrastructure

- `docker-compose.yml` runs PostgreSQL 16, Kafka 3.9 in single-node KRaft mode, MinIO, bootstrap, API, and optional producer services.
- `infra/docker/python.Dockerfile` creates a non-root Python application image.
- Local ports bind to loopback, which is appropriate for the unauthenticated development stack.
- Named volumes preserve PostgreSQL, Kafka, and MinIO state.

## Quality toolchain

- Ruff provides formatting and linting from `pyproject.toml`.
- pytest and pytest-asyncio cover contracts, generation, API behavior, producer behavior, and live dependencies.
- `.github/workflows/ci.yml` runs locked installs, static checks, unit tests, image builds, and Compose integration tests.
- Baseline on 2026-09-18: 40 tests passed and 5 integration tests were deselected.

## Not yet implemented

- Spark, dbt, Airflow, Redis, React/TypeScript, Prometheus, Grafana, OpenTelemetry, Terraform, and AWS resources are documented as planned, not present dependencies.
- PostgreSQL has no application or analytical tables yet.
- MinIO has no pipeline data writer yet.

*Stack analysis: 2026-09-18*
