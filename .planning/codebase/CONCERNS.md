# Technical Concerns

**Analysis Date:** 2026-09-18

## Highest priority

- The target data platform stops at Kafka production; no streaming consumer, warehouse model, or analytical API exists.
- PostgreSQL and MinIO are listed as implemented foundations but hold no application data beyond test objects.
- The precreated dead-letter topic is unused, so corrupt records remain only in the source topic.
- Replay behavior is described but not yet proven at a sink.

## Data correctness

- Duplicate application events are deliberate and require `event_id`-based sink deduplication.
- Shipment delays must update shipment cohorts rather than create independent shipments.
- Refund requests are not settled refunds and must remain separate in warehouse semantics.
- Revenue must use successful payment events rather than order creation amounts.
- Failure rates need payment attempts as their denominator.

## Reliability and operations

- Kafka is single-node and cannot demonstrate broker high availability.
- There is no consumer checkpoint, backpressure, late-data policy, or replay runbook.
- No service exposes Prometheus metrics or distributed traces.
- Readiness probes are solid, but there are no user-facing data endpoints to observe yet.

## Security

- The local API has no authentication or TLS and must remain loopback-only.
- Kafka and MinIO use plaintext local networking inside Compose.
- Secrets are handled safely in source, but future Spark/Airflow/dbt containers must follow the same pattern.
- No dependency vulnerability scan is configured.

## Developer experience

- Compose needs roughly 4 GB before Spark/Airflow are added; a profile strategy will be important.
- `terraform` is not installed on the audit host, so future validation needs either CI or a tool install.
- Ruff cache writes emitted Windows/OneDrive permission warnings despite successful checks.

## Documentation risk

- The README is unusually honest and should preserve its implemented/planned distinction.
- Target architecture references remain defensible only if each phase updates the status table immediately.
- No performance, scale, deployment, user, or AI accuracy claims should be introduced without evidence.

*Concern analysis: 2026-09-18*
