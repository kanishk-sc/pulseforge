# PulseForge: evidence before dashboards

PulseForge is a deliberately synthetic commerce/logistics workload, not a
production merchant system. A regional payment failure, delayed shipment or
refund spike can be obscured by aggregate metrics. The project asks a data
engineering question: can an operator trace a visible finding back through a
tested metric and durable event identity without hiding partial builds or
replay effects?

## Architecture and ownership

The [versioned event contract](../../src/pulseforge/events.py) and bounded
[producer](../../src/pulseforge/producer.py) generate correlated journeys.
Kafka decouples acknowledgement from analytics. [Spark queries](../../src/pulseforge/streaming/queries.py)
archive original payloads, validate and dead-letter malformed records, and
write cleaned/curated Parquet to MinIO. A transactional
[warehouse sink](../../src/pulseforge/streaming/warehouse.py) stores events,
source coordinates, batch ledger and minute metrics in PostgreSQL. The
[Phase 2 design](../architecture/phase-2-design.md) defines the ownership:
Spark checkpoints control stream progress; PostgreSQL event-ID and
topic/partition/offset uniqueness are the durable replay backstop. A Spark
checkpoint is not a distributed transaction across Kafka, object storage
and SQL. Very late records remain in raw evidence even when the ten-minute
live watermark excludes them from cleaned/business views.

[dbt](../../analytics) models event-grain facts and hourly marts, with tested
denominators such as payment attempts rather than every commerce event.
[Airflow's finite DAG](../../airflow/dags/pulseforge_analytics.py) verifies
the warehouse, runs dbt, and summarizes quality; it never starts, stops or
owns Spark. The [product pipeline](../../src/pulseforge/product/cli.py)
serializes a build, tests it, and publishes a complete immutable generation.
The [API](../../src/pulseforge/api.py) serves that build's metrics/status
and persisted detector evidence; build-aware Redis keys can be bypassed
when Redis is unavailable. The [detectors](../../src/pulseforge/product/detectors.py)
require complete, sufficiently sampled baseline/evaluation windows.
The [React UI](../../frontend/src/App.tsx) shows freshness, empty/error states
and original-build source records. The optional
[assistant](../../src/pulseforge/assistant/service.py) provides an offline,
read-only evidence summary with citations; its hypotheses are not detector
decisions or proven causes.

## Recovery is a coordinated snapshot, not magic replay

The Phase 7 [release tool](../../scripts/deployment/release.py) tags locally
built images by source SHA, verifies their IDs and refuses registry fallback
for missing application tags. The [recovery tool](../../scripts/deployment/recovery.py)
requires quiesced writers and archives six named volumes together so SQL,
Kafka, lake objects and Spark checkpoints refer to one stopped state. It
restores into a **different** project and retains a partial target for
diagnosis if extraction fails. The read-only
[state probe](../../scripts/deployment/state_probe.py) compared SQL identities,
build/evidence/corpus state and representative raw/cleaned/curated,
commit-manifest and checkpoint contents by SHA-256.
The [local acceptance record](../deployment/acceptance.md) observed 190
unique event IDs/source positions after restored Spark restarted, with no
new duplicate business effect. It did not hash every object, measure
RPO/RTO or test host loss on AWS.

## Three failures that changed the design

| Failure observed | Repair and regression evidence |
| --- | --- |
| The first partition of the raw lake exposed a mismatch between the declared schema and its `ingest_date` partition. | The source schema gained the explicit partition field; the fresh-volume Spark/lake integration path now exercises the first partition. See [Phase 2 verification](../verification.md#failures-found-during-implementation) and the [actual-producer lake test](../../tests/test_streaming_integration.py). |
| The first persisted incident used `Connection.executemany`, but Psycopg 3 provides that operation on a cursor. The transaction rolled back rather than silently leaving half an incident. | The [detector](../../src/pulseforge/product/detectors.py) uses a cursor for evidence insertion. [Product integration](../../tests/test_product_integration.py) and [Phase 4 verification](../verification.md) establish one incident, 20 evidence rows and idempotent reevaluation. |
| A migration-lock check assumed tuple rows, while the product integration connection used `dict_row`; the first Phase 7 hosted product job exposed the mismatch. | Both [product](../../src/pulseforge/product/schema.py) and [assistant](../../src/pulseforge/assistant/corpus.py) migrations read the advisory-lock result through a tuple-row cursor. The [disposable-database test](../../tests/test_deployment_integration.py) covers dict-row connections, competing locks and reruns; the [final PR CI](https://github.com/kanishk-sc/pulseforge/actions/runs/36477116362) passed. |

The Phase 7 review additionally closed a release-safety hole: absent local
images now fail instead of being pulled from a registry; see the
[focused guard tests](../../tests/test_deployment.py) and
[review evidence](../deployment/acceptance.md#review-checks-2026-09-28).

## Tradeoffs and rollout boundary

For this small workload, a single transactional consumer and a relational
database could replace Kafka, Spark, MinIO and Airflow at much lower operating
cost. This project uses them to make ordering, event time, recovery,
publication and ownership boundaries observable. The local stack has one
Kafka broker, one Spark driver, one PostgreSQL server and one host target;
it is not highly available. The [Terraform design](../architecture/phase-7-design.md)
is private EC2/EBS/S3 configuration validated without credentials, not a
deployed cloud system. A real rollout still needs account-aware security
review, workload identity and secrets, application authentication/internal
TLS, capacity/load tests, backup retention and full-object recovery drills,
cloud RPO/RTO measurement, operational ownership and cost approval.
Live-provider explanation quality also remains unverified. No throughput,
latency, savings, availability or user-count claim is inferred from the
synthetic local tests.
