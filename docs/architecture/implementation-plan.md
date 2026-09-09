# PulseForge implementation checklist

PulseForge is a synthetic commerce/logistics operations platform. Build and verify
each milestone before expanding its scope. Checkmarks mean verified, not scaffolded.

## 1. Foundation (current milestone)
- [x] Architecture, local setup, failure semantics and repository conventions
- [x] Versioned Pydantic event contracts for all eight event types
- [x] Seeded, correlated event generator with explicit anomaly scenarios
- [x] Kafka producer: delivery confirmation, bounded retries and graceful shutdown
- [x] Compose: PostgreSQL, KRaft Kafka, MinIO, initialization and API health checks
- [x] FastAPI liveness/readiness, typed responses and request IDs
- [x] Unit, API, schema and opt-in infrastructure integration tests
- [x] Lint, tests, image builds and live foundation smoke test
- [x] Stable foundation commit (`c0b1884`)

## 2. Streaming data platform
- [ ] Spark Kafka consumer with checkpoints and replay tests
- [ ] Preserve raw bytes, schema validation and dead-letter reasons
- [ ] Watermark-aware deduplication plus warehouse uniqueness for durable idempotency
- [ ] Enrichment and raw/cleaned/curated Parquet on S3-compatible storage
- [ ] Transactional warehouse writes and window aggregates
- [ ] Test duplicate replay, malformed payloads, restarts and sink outages

## 3. Analytics engineering
- [ ] dbt facts: orders, payments, shipments, refunds
- [ ] Dimensions: customer, product, region; marts: revenue, failures, shipments, refunds, health
- [ ] dbt relationships, uniqueness, accepted values and business-rule tests
- [ ] Airflow transformations, quality reports, ingestion, aggregation and cleanup DAGs
- [ ] Execute dbt and DAG validation against populated warehouse

## 4. Product layer
- [ ] Typed metrics, incident, pipeline and quality APIs
- [ ] Redis cache with bounded TTL and graceful cache failure
- [ ] Explainable anomaly detectors with minimum samples and baseline evidence
- [ ] React/TypeScript dashboard, polling, empty/error states and critical UI tests
- [ ] End-to-end anomaly → persisted incident → dashboard demonstration

## 5. Observability and reliability
- [ ] Prometheus, Grafana, OpenTelemetry and correlation across services
- [ ] Lag, throughput, processing latency, errors, quality and anomaly telemetry
- [ ] Load tests, failure drills and actual benchmark artifacts with environment metadata

## 6. AI operations
- [ ] Operational runbooks, ingestion, embeddings and pgvector retrieval
- [ ] Context includes runbooks, incidents, metrics and pipeline metadata
- [ ] Provider interface, optional real provider and honest offline evidence summary
- [ ] At least 30 evaluation cases with relevance, grounding, identification and latency
- [ ] Persist actual evaluation output; separate automated proxies from human judgments

## 7. Deployment engineering — gated on working local platform
- [ ] Kubernetes deployments/services/config/probes/resources for owned services
- [ ] Terraform managed AWS architecture; validation only, never automatic apply
- [ ] Cost guidance, production security, recovery and scaling documentation

## Verification policy
Run Ruff format/check and pytest after code changes. Run relevant services and record
commands and results in `docs/verification.md`. Never claim a phase is operational
from configuration parsing alone. No invented screenshots, benchmark or AI scores.
