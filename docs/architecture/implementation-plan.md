# PulseForge implementation checklist

PulseForge is a synthetic commerce/logistics operations platform. Build and verify
each milestone before expanding its scope. Checkmarks mean verified, not scaffolded.

## 1. Foundation (complete)
- [x] Architecture, local setup, failure semantics and repository conventions
- [x] Versioned Pydantic event contracts for all eight event types
- [x] Seeded, correlated event generator with explicit anomaly scenarios
- [x] Kafka producer: delivery confirmation, bounded retries and graceful shutdown
- [x] Compose: PostgreSQL, KRaft Kafka, MinIO, initialization and API health checks
- [x] FastAPI liveness/readiness, typed responses and request IDs
- [x] Unit, API, schema and opt-in infrastructure integration tests
- [x] Lint, tests, image builds and live foundation smoke test
- [x] Stable foundation commit (`c0b1884`)

## 2. Streaming data platform (complete)
- [x] Spark Kafka consumer with checkpoints and replay tests
- [x] Preserve raw bytes, schema validation and dead-letter reasons
- [x] Watermark-aware deduplication plus warehouse uniqueness for durable idempotency
- [x] Enrichment and raw/cleaned/curated Parquet on S3-compatible storage
- [x] Transactional warehouse writes and window aggregates
- [x] Test duplicate replay, malformed payloads, restarts and sink outages

Verified on 2026-09-09: 73 local tests on fresh volumes; hosted CI passed 59
unit/API/schema tests and 14 live integration tests. See [evidence](../verification.md).
Phase 3 evidence is recorded separately below.

## 3. Analytics engineering (complete)
- [x] dbt facts: orders, payment attempts, created shipments and refund requests
- [x] Type 1 customer/product and fixed region dimensions; five hourly operational marts
- [x] Source, relationship, uniqueness, accepted-value and singular business-rule tests
- [x] Finite Airflow warehouse verification → dbt build → quality-summary DAG; Spark excluded
- [x] Deterministic populated-warehouse, replay, rerun and real Airflow DAG validation

Re-verified on 2026-09-16: 68 non-integration tests, four deterministic analytics
tests, 109 successful dbt build results, a successful real Airflow DAG run and 86
full local tests including every Phase 2 streaming recovery case. Hosted CI passed
all three jobs after correcting the MinIO registry reference. See [design](phase-3-design.md) and
[evidence](../verification.md).

## 4. Product layer (complete)
- [x] Typed metrics, pipeline and quality APIs backed by dbt marts
- [x] Redis cache with bounded TTL and graceful cache failure
- [x] Atomic successful-build publication and explicit stale/failed/running states
- [x] Persisted incident API
- [x] Explainable anomaly detectors with minimum samples and baseline evidence
- [x] React/TypeScript dashboard, polling, typed data, and loading/empty/error states
- [x] Critical component and real browser UI tests
- [x] End-to-end anomaly → persisted incident → dashboard demonstration

Verified on 2026-09-20 with a 140-event deterministic fixture, 109 successful dbt
results, an immutable product publication, one evidence-backed critical incident,
live PostgreSQL/API assertions and an actual browser flow. See
[design](phase-4-design.md) and [evidence](../verification.md).

## 5. Observability and reliability
- [x] Prometheus API metrics, provisioned Grafana dashboard and optional FastAPI OpenTelemetry export
- [x] API latency/errors, cache outcomes, warehouse failures and modeled throughput/quality views
- [x] Spark driver processed offsets/progress, retry-aware attempt metrics and durable warehouse state; no false consumer-group lag claim
- [x] Finite analytics/detector run state, local alert rules, Tempo traces and provisioned operations dashboards
- [x] Bounded local load/failure acceptance and actual benchmark artifacts with environment metadata

Local Phase 5 evidence is in [verification](../verification.md) and the
[bounded measurement report](../benchmarks/phase5-report.md). Hosted PR checks are
tracked separately by GitHub Actions; these local results are not production SLAs.

## 6. Evidence-bound AI operations
- [x] Allowlisted versioned runbooks, finite ingestion, pinned local embeddings and real pgvector retrieval
- [x] Bounded typed original-build incident evidence, current publication status labeled separately
- [x] Clearly labeled offline evidence summary in the API and existing React incident detail
- [x] Disabled-by-default provider interface and real OpenAI Responses adapter with mocked failure-contract tests
- [x] 32 original retrieval cases and a fresh eight-case review holdout with machine-readable offline results; the original held-out split became review-exposed
- [ ] Live provider inference and semantic grounding/usefulness human review (requires separate authorization/review)

The checked items describe local implementation and offline evidence only. A passing
mocked adapter test does not establish live provider compatibility or model quality.
See [Phase 6 design](phase-6-design.md), [evaluation rubric](../assistant/evaluation.md)
and [verification](../verification.md).

## 7. Deployment engineering — gated on working local platform
- [ ] Kubernetes deployments/services/config/probes/resources for owned services
- [x] Terraform AWS target for ECS, ALB, RDS, ElastiCache, S3, ECR, IAM and logs; validated but never applied
- [x] Demo cost trade-offs, production security gaps and state/deletion safeguards documented
- [ ] Recovery drills, autoscaling policy and measured capacity guidance

## Verification policy
Run Ruff format/check and pytest after code changes. Run relevant services and record
commands and results in `docs/verification.md`. Never claim a phase is operational
from configuration parsing alone. No invented screenshots, benchmark or AI scores.
