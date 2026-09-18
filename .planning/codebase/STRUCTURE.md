# Repository Structure

**Analysis Date:** 2026-09-18

## Root

- `README.md` accurately separates the implemented foundation from planned phases.
- `pyproject.toml` defines the Python package, dependencies, lint rules, and pytest markers.
- `uv.lock` makes dependency resolution reproducible.
- `docker-compose.yml` defines all currently runnable services.
- `.env.example` documents required local settings without credentials.
- `Makefile` provides optional shortcuts for setup, linting, tests, and Compose.

## Application package

- `src/pulseforge/events.py` owns event types and validation.
- `src/pulseforge/generator.py` owns synthetic business journeys and corruption injection.
- `src/pulseforge/producer.py` owns Kafka delivery and shutdown behavior.
- `src/pulseforge/bootstrap.py` owns idempotent topic and bucket setup.
- `src/pulseforge/api.py` owns HTTP lifecycle, request context, and health endpoints.
- `src/pulseforge/dependencies.py` owns dependency probes and S3 client construction.
- `src/pulseforge/config.py` owns environment-derived settings.
- `src/pulseforge/logging.py` owns structured JSON logging.

## Tests and schemas

- `tests/test_events.py` covers contract rejection and serialization.
- `tests/test_generator.py` covers repeatability, journeys, scenarios, and corruption.
- `tests/test_producer.py` covers delivery semantics, bounded retries, and shutdown.
- `tests/test_api.py` covers liveness, readiness, request IDs, and error disclosure.
- `tests/test_integration.py` covers live PostgreSQL, Kafka, MinIO, bootstrap, and API readiness.
- `schemas/commerce-event.v1.json` is checked against the Pydantic model in CI.

## Infrastructure and documentation

- `infra/docker/python.Dockerfile` is the only infrastructure file below `infra/`.
- `docs/architecture/architecture.md` records target decisions and boundaries.
- `docs/architecture/implementation-plan.md` tracks the existing phased roadmap.
- `docs/verification.md` records foundation verification, not benchmark claims.
- `.github/workflows/ci.yml` is the sole CI workflow.

## Missing target directories

- There are no `spark/`, `dbt/`, `airflow/`, `frontend/`, `observability/`, or `infra/terraform/` implementations yet.

*Structure analysis: 2026-09-18*
