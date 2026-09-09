# Architecture and engineering decisions

## Scope and constraints

PulseForge uses entirely synthetic commerce/logistics data. Phase 1 establishes a
working ingestion boundary. Phase 2 adds the [streaming implementation](phase-2-design.md),
including sink, checkpoint, watermark and recovery semantics. Phase 3 adds the
[analytics implementation](phase-3-design.md), including exact model grains, metric
denominators, idempotent rebuilds and finite orchestration. The seven-phase
[plan](implementation-plan.md) distinguishes implemented code from future architecture.
The design prioritizes reproducibility, explicit failure behavior and useful tests.

## Kafka versus synchronous ingestion

Kafka is the ingestion boundary because producers should not wait for Spark, warehouse
or AI availability. A durable log supports independent consumers and replay. That
benefit costs broker operations and eventual consistency. A small CRUD application
would be better served by synchronous writes; this project deliberately exercises
streaming recovery and analytical freshness instead.

Local Kafka runs one KRaft broker/controller, three partitions and replication factor
one, with auto topic creation disabled. Internal clients use `kafka:29092`; host clients
use `localhost:9092`. Correct advertised listeners matter: Kafka redirects a connected
client to partition leaders. A Docker-only hostname cannot work for a host consumer.
Retention is seven days. Single-node storage loss is unrecoverable without backups;
production would require multiple brokers, replicated partitions and TLS/SASL.

The producer partitions by order ID (or customer/product for non-order records), uses
`acks=all` and idempotent broker sequencing, and counts records only after confirmation.
An ambiguous 30-second delivery timeout stops the producer. It does not blindly retry
under a new producer ID. Startup retries are bounded, shutdown drains with a bound,
and SIGTERM requests a graceful stop. Throughput is deliberately modest until measured;
concurrent/batched dispatch can come later if profiling justifies it.

## Contracts and data quality

Version 1 requires stable UUID event IDs, recognized types/regions, timezone-aware
timestamps (2020 onward, at most five minutes into the future), appropriate status and
event-specific foreign IDs/providers. Decimal amounts must be finite, nonnegative and
have at most two fractional digits. Inventory updates require nonnegative integer
quantity. Unknown fields and unsupported versions fail validation rather than silently
changing meaning. Replay older than 2020 is outside this synthetic contract.

Schemas alone do not detect duplicate delivery or plausible-but-wrong business values.
Phase 2 handles those at the stream/sink boundaries. Duplicates must be measured,
not confused with malformed records. Large transactions remain valid; detection is a
business decision rather than a schema rule. JSON Schema is generated from the model,
but cross-field/time-dependent checks remain runtime Python validation.

## Streaming versus batch, and Spark's role

Spark Structured Streaming handles continual microbatches from Kafka, event-time
windows, enrichment and checkpointed offsets. It is chosen to demonstrate distributed
processing semantics and unified Parquet transformations, not because the local
generator requires a cluster. A simpler consumer would be cheaper at this local scale.

Airflow schedules one finite hourly chain: verify the warehouse connection, run the
complete dbt build and tests, then summarize machine-readable quality results. It does
not loop as the streaming consumer or manage Spark. Spark checkpoints own streaming
progress; bounded Airflow task retries own analytics recovery. Runbook ingestion and
retention cleanup remain out of scope.

## Lake layers and AWS portability

The foundation provisions the `pulseforge` S3-compatible bucket. Phase 2 writes:

| Prefix | Meaning | Replay/quality behavior |
| --- | --- | --- |
| `raw/` | Original Kafka payload plus topic, partition, offset and ingest timestamp | Preserve malformed bytes too; never silently discard evidence |
| `cleaned/` | Valid, normalized, enriched events in Parquet | Keep schema version and event ID; explicit rejection reasons elsewhere |
| `curated/` | Normalized events with date/hour and operational flags | Minute aggregates are stored in PostgreSQL |

Use event date/type partitions, bounded file counts and eventual compaction. Avoid
high-cardinality IDs in paths. A single endpoint/region/bucket configuration keeps
application code portable; production S3 should use workload roles rather than static
MinIO root keys. Phase 1 uses local credentials only; IAM support belongs with AWS
deployment work. MinIO is pinned to a published community image for the local demo;
review upstream security fixes and licensing before any production deployment.

## Warehouse modeling

Store durable event IDs with a unique constraint and use transactional upserts. Spark's
checkpoint alone cannot provide exactly-once behavior across multiple external sinks.
Write each sink idempotently and reconcile partial progress on replay. Avoid claiming
a distributed transaction between Kafka, Parquet and PostgreSQL.

dbt staging preserves the complete Phase 2 event and minute-metric sources in UTC.
Type 1 dimensions represent observed customer/product IDs and the four contract regions.
Facts represent order creation, payment results, shipment creation and refund requests,
with every row linked to its source event UUID. A shipment delay updates attributes on
the created shipment instead of creating another shipment. A refund request is not a
completed refund.

Revenue sums successful payments only. Payment failure rate divides failed results by
all processed plus failed attempts. Delayed shipment rate divides shipment creations
with at least one delay by all creations in the same creation-hour cohort. Marts name
those semantics, use UTC event-time hours and retain null when no denominator exists.
The combined health mart is a feature table, not anomaly detection. See the
[Phase 3 design](phase-3-design.md) for the full model table and late-arrival behavior.

## API, caching and failures

Phase 1 has two typed health endpoints. `/health` is process liveness; `/ready` concurrently
checks PostgreSQL, Kafka topic existence and the bucket. It returns 503 when any check
fails. Bounded socket/query deadlines prevent an outage from hanging requests. SQLAlchemy
owns a bounded async connection pool with pre-ping and lifespan cleanup. boto3 probes
run in threads because its client is synchronous. Exception types, not credentials or
raw connection strings, appear in application error logs.

The API is local-only and read-only today. Authentication, authorization, TLS, rate
limits and least-privilege service credentials are required before external exposure.
Redis will cache expensive aggregate reads with short TTLs, not authoritative incidents
or health state. A cache outage should degrade to database reads with load protection.

## Observability and explainable incidents (planned)

Current JSON logs include request IDs and request duration; health endpoints expose
dependency state. Later telemetry must distinguish Kafka input rate, consumer lag,
Spark batch duration, invalid/duplicate records, warehouse freshness, API errors and
latency, anomaly counts and AI latency. Never label metrics with event/customer IDs:
that creates unbounded cardinality. Use those IDs in logs/traces instead.

Detectors will require minimum sample sizes, trailing historical baselines and cooldowns.
Incidents persist observed values, baselines, affected segments and source evidence.
An order-volume drop needs a time-driven check even when no new events arrive.
The AI assistant will retrieve this evidence and runbooks; it must not invent a cause
from correlation or act as the primary detector. Answers will distinguish observation,
hypothesis and recommended investigation. Offline mode remains a labeled evidence
summary, and future evaluations will report only executed measurements.

## Scale and deployment gate

Scale Kafka partitions based on key distribution; monitor hot keys. Scale Spark workers
only once batch duration/lag warrant it. PostgreSQL favors batched writes, indexes for
actual query shapes and bounded API concurrency. Raw object storage supports long-term
replay even after Kafka retention expires. Backups and restore drills are separate from
replication; both are needed for production reliability.

Kubernetes configuration and Terraform are deliberately deferred. The intended AWS
mapping is S3, managed PostgreSQL, managed Kafka and container compute, with workload
identity and managed observability. No cloud account or paid provider is required for
local development. Phase 7 will document costs before any apply instructions.

## References

- [Kafka 3.9 quick start and KRaft](https://kafka.apache.org/39/getting-started/quickstart/)
- [FastAPI lifespan lifecycle](https://fastapi.tiangolo.com/advanced/events/)
- [aiokafka producer delivery semantics](https://aiokafka.readthedocs.io/en/stable/producer.html)
- [MinIO release history](https://github.com/minio/minio/releases)
