# PulseForge résumé and interview sheet

This is wording for a **synthetic, locally verified portfolio project**,
not a claim of employment, production users, deployed AWS resources or
measured business impact. Adjust first-person wording only if it accurately
describes your contribution.

## Project description

Built a synthetic commerce data platform that traces Kafka events through
Spark, Parquet and PostgreSQL to tested dbt marts, build-aware APIs,
deterministic incidents and an evidence-focused React dashboard. Local
replay, failure and restore drills plus hosted CI test the reliability
contracts; the private AWS target is configuration only.

## Résumé bullets

Data engineering:

- Implemented versioned event validation, Kafka ingestion, Spark raw/cleaned/
  curated paths and a dead-letter stream; combined checkpointed processing
  with transactional PostgreSQL event/offset uniqueness to make replay
  effects testable. [Streaming design](../architecture/phase-2-design.md) ·
  [integration tests](../../tests/test_streaming_integration.py)
- Modeled order, payment-attempt, shipment and refund facts in dbt with
  explicit grains/denominators and data-quality tests; separated a finite
  Airflow analytics DAG from Spark's continuous checkpoint ownership.
  [Analytics design](../architecture/phase-3-design.md) ·
  [verification](../verification.md)
- Built an immutable successful-build publication path and deterministic
  incident evidence; exercised a local six-volume quiesced restore and
  read-only SQL/object comparison without asserting cloud recovery guarantees.
  [Product design](../architecture/phase-4-design.md) ·
  [deployment acceptance](../deployment/acceptance.md)

Optional backend/data-platform emphasis:

- Exposed versioned, bounded FastAPI metrics and incident routes with
  build-aware Redis caching, explicit stale/unavailable states and
  direct-warehouse fallback on cache failure.
  [API](../../src/pulseforge/api.py) · [product tests](../../tests/test_product_api.py)
- Added a read-only offline incident explanation that links persisted
  original-build evidence to versioned runbooks, labels uncertainty, and
  keeps live provider calls disabled in the verified path.
  [Assistant design](../architecture/phase-6-design.md) ·
  [evaluation limits](../assistant/evaluation.md)

## 60-second explanation

“PulseForge is a synthetic commerce platform I use to demonstrate data
engineering reliability. A producer sends versioned events to Kafka. Spark
archives the original bytes, validates and dead-letters bad records, then
writes Parquet and a PostgreSQL sink with durable event and source-offset
uniqueness. dbt builds tested business marts; a finite Airflow DAG handles
those analytics jobs but never controls the continuous Spark stream. Only a
complete successful build becomes visible to the API and dashboard.
Deterministic detectors persist an incident together with its source-event
evidence, and an optional offline explanation cites that original build and
runbooks. I tested replay, warehouse outages, failed publication and an
isolated local backup/restore. It is not a production deployment: the AWS
infrastructure is only statically validated, and the local stack is
single-node.”

## Ten likely technical questions

1. **What happens on duplicate delivery?** The producer uses idempotent
   broker sequencing within its session; Spark deduplicates within its
   event-time state, and PostgreSQL uniquely constrains both event IDs and
   Kafka source coordinates. Replaying the same source position cannot
   double-count a committed business row. [Sink](../../src/pulseforge/streaming/warehouse.py)

2. **What happens to late data?** Original bytes remain in the raw archive.
   The ten-minute watermark bounds the live deduplication state; sufficiently
   late events may not enter cleaned/warehouse results. The project records
   drops and does not claim automatic historical reconciliation.
   [Phase 2 design](../architecture/phase-2-design.md)

3. **Which time drives analytics?** Source event time drives the modeled
   hourly business window. Ingest time describes when Kafka data was
   archived; publication time describes when a tested build became visible.
   They are distinct UTC timestamps. [Phase 3 design](../architecture/phase-3-design.md)

4. **Why a watermark, and what is its cost?** It bounds state for event-time
   deduplication instead of retaining every ID forever. The cost is that a
   very late event can remain only in raw evidence until an explicit
   reconciliation path exists. [Late-event test](../../tests/test_streaming_integration.py)

5. **Where are transaction boundaries?** A warehouse microbatch stages and
   merges events/ledger/metrics in a PostgreSQL transaction. Kafka offsets,
   object writes and SQL do not share a distributed transaction, so
   checkpoints plus idempotent sinks and reconciliation handle partial
   progress. Publication never exposes a partially tested dbt generation.
   [Streaming design](../architecture/phase-2-design.md) ·
   [publication tests](../../tests/test_product_publication.py)

6. **What are the dbt grains and payment denominator?** Facts are at source
   event/order/payment-attempt/created-shipment/refund-request grains as
   documented; hourly payment failure rate divides failed attempts by
   eligible payment attempts, while revenue counts successful payments only.
   A zero denominator is explicit, not a fabricated zero rate.
   [Phase 3 design](../architecture/phase-3-design.md)

7. **Why is Airflow separate from Spark?** Spark is a long-running,
   checkpoint-owning stream. Airflow runs a finite warehouse check, dbt
   build/test and quality summary; it neither restarts Spark nor mutates
   its checkpoint. [DAG](../../airflow/dags/pulseforge_analytics.py)

8. **What if Redis is down?** Redis holds short-lived, build-aware cached
   responses; PostgreSQL and the published build remain authoritative.
   Bounded cache failures bypass Redis and read the warehouse. A local
   Redis-stop exercise preserved the API response; it is not a throughput
   promise. [Verification](../verification.md)

9. **What can observability prove?** Request/stream/finite-job metrics and
   optional traces help locate failures, while readiness probes report
   dependencies. A healthy container or heartbeat cannot prove that data
   is fresh end to end; the product surfaces build freshness separately.
   [Signal contract](../architecture/phase-5-design.md)

10. **Why trust an explanation?** Detection is rule-based, not generated.
    The offline explanation reads bounded persisted evidence from the
    incident's original build, separates facts from hypotheses, and attaches
    citation IDs. A valid citation or retrieved section is still not proof
    of causal correctness; human grounding/usefulness review remains open.
    [Assistant design](../architecture/phase-6-design.md)
