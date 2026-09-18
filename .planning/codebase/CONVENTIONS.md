# Engineering Conventions

**Analysis Date:** 2026-09-18

## Python style

- Python targets 3.12 and uses modern typing, `StrEnum`, and Pydantic 2 APIs.
- Ruff enforces import ordering, common correctness rules, upgrades, bugbear, and async checks.
- Lines target 100 characters.
- Functions and modules use snake_case; models and enums use PascalCase.
- Public modules generally have narrow responsibilities.

## Configuration

- Runtime configuration is environment-driven through `Settings`.
- Secrets use Pydantic `SecretStr` and are not interpolated into logs.
- `.env` is ignored while `.env.example` is explicitly tracked.
- Compose refuses to start when required passwords are blank.

## Error handling

- API middleware converts unexpected exceptions to a stable 500 response with a request ID.
- Dependency readiness reports only `up` or `down`, avoiding credential-bearing exception details.
- Producer startup retries only bounded connection failures.
- Ambiguous Kafka delivery timeouts are surfaced instead of blindly republished.
- Bootstrap treats existing topics and buckets as successful idempotent state.

## Data modeling

- Pydantic models forbid extra event fields and are immutable after validation.
- Decimal is used for monetary amounts.
- UTC-aware timestamps are required and bounded.
- Type-specific business rules live alongside the event model.

## Logging

- Application logs are JSON and include a UTC timestamp, level, logger, and event name.
- Selected structured fields include request IDs, HTTP status, duration, event IDs, and error types.
- Exception messages are intentionally omitted from JSON logs.

## Documentation conventions

- Status language distinguishes implemented, planned, and not deployed.
- Local development constraints and lack of authentication/TLS are explicit.
- Verification claims name the commands and boundaries.

*Convention analysis: 2026-09-18*
