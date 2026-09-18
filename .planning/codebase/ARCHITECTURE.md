# Architecture

**Analysis Date:** 2026-09-18

## Current pattern

- PulseForge is a small event-platform foundation with separately runnable bootstrap, producer, and API entry points.
- Shared configuration and the event contract live in one Python package.
- Kafka is the durable event backbone; PostgreSQL and MinIO are currently verified dependencies rather than data sinks.
- The API is operational infrastructure today, not an analytics product API.

## Event flow

1. `EventGenerator` creates a correlated synthetic commerce journey.
2. `CommerceEvent` validates schema, identifiers, timestamps, money, and type-specific requirements.
3. `EventGenerator.next_record` may deliberately corrupt transport records for downstream resilience testing.
4. `producer.run` keys and publishes records to the configured Kafka topic.
5. No consumer currently transforms, rejects, archives, or persists those records.

## Service lifecycle

- `bootstrap.initialize` creates Kafka topics and the S3 bucket idempotently.
- FastAPI lifespan creates and disposes the async database engine.
- Liveness is independent of dependencies; readiness checks PostgreSQL, Kafka, and object storage concurrently.
- Producer shutdown is cooperative and always attempts to close the Kafka producer.

## Contract boundaries

- `src/pulseforge/events.py` is the authoritative runtime event contract.
- `schemas/commerce-event.v1.json` is a generated artifact and cannot express every business validator.
- Event IDs are UUIDs and event amounts are decimal values serialized without binary floating-point loss.
- Order IDs correlate payment, shipment, and refund events within generated journeys.

## Intended next boundary

- Spark Structured Streaming should become the only initial consumer of `commerce.events.v1`.
- It should retain Kafka metadata, preserve `event_id`, classify invalid records, and write replay-safe raw/curated sinks.
- Warehouse tables should provide stable sources to dbt without asking Airflow to own the streaming lifecycle.

## Architectural risks

- One Kafka broker is intentionally not highly available.
- Cross-restart producer duplicates are possible and must be handled at sinks.
- No migration mechanism exists for the future warehouse schema.
- The repository has not yet demonstrated late data, checkpoint recovery, or sink idempotency.

*Architecture analysis: 2026-09-18*
