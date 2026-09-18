# Testing Strategy

**Analysis Date:** 2026-09-18

## Framework and commands

- pytest 9 and pytest-asyncio drive Python tests.
- `uv run pytest -m "not integration"` is the default fast suite.
- `uv run pytest -m integration --run-integration` opts into live Compose tests.
- `tests/conftest.py` skips integration tests unless the explicit flag is present.

## Unit coverage

- Event tests reject malformed identifiers, money, timestamps, enums, status mismatches, and extra fields.
- Generator tests prove deterministic output for fixed seeds and correlated journey identity.
- Scenario tests validate materially different payment failure behavior.
- Corruption tests ensure duplicate, malformed, and missing-ID records are produced and rejected by the contract.
- Producer tests mock Kafka to prove acknowledged sends, bounded startup retries, no ambiguous retry, and shutdown.

## API coverage

- Liveness works without external services.
- Readiness returns 200 or 503 based on dependency state.
- Oversized/untrusted request IDs are replaced.
- Unexpected errors do not expose sensitive details.
- OpenAPI includes documented health responses.

## Integration coverage

- The live suite verifies API readiness against running services.
- Bootstrap is invoked twice to exercise idempotency.
- PostgreSQL executes a transaction against a temporary table.
- MinIO performs an object put/get/delete round trip.
- Kafka performs an acknowledged produce and exact readback.

## Baseline result

- On 2026-09-18, formatting, lint, schema drift, and all 40 non-integration tests passed locally.
- Five integration tests were collected but not yet run during the initial audit.
- Compose configuration validates after generating an ignored `.env`.

## Gaps for the next phase

- Spark parsing, dead-letter routing, watermarking, checkpoint recovery, and sink deduplication need tests.
- Testcontainers or a targeted Compose profile may reduce the cost of pipeline integration tests.
- No coverage percentage is configured or claimed.

*Testing analysis: 2026-09-18*
