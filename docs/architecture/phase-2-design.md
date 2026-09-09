# Streaming design and failure semantics

Phase 2 extends the foundation without adding a product layer or batch orchestration.
Acceptance evidence belongs in [verification](../verification.md).

## Runtime compatibility

| Component | Version | Purpose |
| --- | --- | --- |
| Python | 3.12 | Shared contracts and services |
| Spark / PySpark | 4.0.1 | Structured Streaming, state, Parquet and JDBC |
| Java | OpenJDK 17 | Supported Spark JVM |
| Scala Kafka connector | spark-sql-kafka-0-10_2.13:4.0.1 | Matches Spark and Scala binary versions |
| Kafka Java client | 3.9.1 | Transitive connector dependency |
| Hadoop client / AWS | 3.4.1 | Matches bundled Hadoop and enables S3A |
| AWS SDK bundle | 2.24.6 | Hadoop AWS transitive dependency |
| PostgreSQL JDBC | 42.7.7 | Distributed staging writes and candidate reads |
| psycopg | Frozen in uv.lock | Control transactions and commit ledger |

`spark-pom.xml` resolves JVM dependencies during Docker build; no Maven downloads
occur at service startup. The Python streaming extra keeps Spark out of the API
image. Startup asserts the Hadoop runtime version matches S3A. Integration tests
execute Kafka, S3A and JDBC, rather than inferring compatibility from installed jars.

One `local[2]` process runs behind the `streaming` Compose profile. Two shuffle
partitions suit a laptop. Spark is used for its checkpointed stateful processing
model and distributed DataFrame transforms, not because this generator requires a
cluster. A normal Python consumer would be simpler at this volume. No horizontal
scalability claim follows from this local configuration.

## Three independent queries

1. **raw** consumes Kafka, retaining source topic/partition/offset/timestamp, key bytes,
   payload bytes and ingestion timestamp. Append-only Parquet goes to
   `raw/v1/ingest_date=...` through Spark's native file sink.
2. **dlq** validates committed raw files with Pydantic and sends rejections to
   `commerce.dead-letter.v1`. Each record has a source identity, safe code/detail,
   and Base64 payload/key. Its Kafka key is `topic:partition:offset`.
3. **valid-events** validates the same raw source, normalizes valid records, applies
   event-time deduplication, and runs the lake/warehouse microbatch sink.

Reading raw after the native file-sink commit makes preservation precede validation.
Malformed JSON, missing IDs, invalid event types, schema failures, business-rule
failures and other non-object payloads have distinct codes. Diagnostics never include
input values or exception stacks. Invalid UTF-8 and non-finite JSON are safe rejections.

Streaming supplies the immutable raw ingestion time as Pydantic's optional reference
clock. Replay therefore cannot change a future-timestamp rejection merely because
wall time advanced. Normal producer/API validation retains its current-time behavior.

## Layers and supported readers

- **Raw:** immutable evidence, including invalid and duplicate records. Spark's
  `_spark_metadata` log excludes incomplete attempts from supported reads.
- **Cleaned:** normalized valid events and source lineage. Money is Decimal(14,2);
  metadata is JSON text in Parquet and JSONB in PostgreSQL.
- **Curated:** UTC event date/hour plus payment-failure, shipment-delay and refund flags.
  These are convenience fields, not dimensional models or anomaly detection.

Cleaned/curated output uses a directory per checkpoint UUID and microbatch. Successful
files are immutable. `commits/v1/<query-id>/<batch>.json` identifies completed outputs.
The host inspection command honors these manifests and raw metadata. Arbitrary
recursive scans may read unfinished attempts; they are not a supported consistent
read method. Manifests can briefly lag the database transaction. Daily raw partitioning
avoids per-customer directories, but microbatch files still accumulate. Compaction and
retention management are future work, not hidden background behavior.

## Event time and distinct idempotency boundaries

The live query uses `withWatermark("event_ts", "10 minutes")` and
`dropDuplicatesWithinWatermark(["event_id"])`. Ten minutes is a demo lateness budget
for delivery jitter and short disruptions, not a measured business SLA. The watermark
tracks maximum observed event time minus the delay; wall-clock waiting alone does
not advance it. Records older than the watermark may be excluded from live sinks.
They remain in raw, and progress logs report `watermark_dropped` counts.

This query is not an unlimited historical backfill engine. A future reconciliation
path must read raw without a live lateness cutoff and honor database uniqueness.
This phase does not claim that offline backfill implementation exists.

| Mechanism | Boundary |
| --- | --- |
| Kafka producer idempotence | Suppresses broker retry duplicates within a producer session, not equal application IDs or independent producer restarts |
| Spark watermark deduplication | Bounds state and suppresses IDs within the lateness horizon; late data may be excluded and IDs can age out |
| PostgreSQL uniqueness | Event primary key and unique source tuple persist across state eviction and restart; first committed version wins |

The sink excludes already stored identities before writing cleaned/curated output.
An event ID reused with conflicting content is treated as the same logical event;
raw retains both payloads. Conflict diagnosis beyond this first-write policy is not
implemented.

## Microbatch protocol

The callback fully consumes and caches the stateful DataFrame, even on committed
retries; skipping consumption can leave Spark state incomplete.

1. Read the stable checkpoint UUID and pair it with batch ID. A new checkpoint cannot
   alias a previous generation's batch zero.
2. Acquire a PostgreSQL session advisory lock for one active warehouse writer.
   Competing writers fail visibly instead of racing lake publication.
3. If the pair is in `streaming_batches`, republish its deterministic manifest and
   return without changing warehouse counts.
4. Recreate a hash-named staging table; Spark JDBC writes the batch. SQL `DISTINCT ON`
   absorbs duplicate stage rows from JDBC task retries. Identifiers are validated hashes.
5. JDBC reads candidates excluding existing event/source identities. Write cleaned
   and curated Parquet. Reuse successful `_SUCCESS` outputs on retry; only incomplete
   attempt directories may be overwritten, never raw evidence.
6. In one transaction: `INSERT ... ON CONFLICT DO NOTHING`, calculate UTC minute totals
   **only from INSERT RETURNING rows**, update aggregates, write the ledger and drop
   the staging table. All these effects commit together or roll back together.
7. Publish the manifest. If publication fails after DB commit, callback replay uses
   step 3 to repair it without changing event/metric counts.

Minute amounts are grouped by event type and are not called revenue: an order,
successful payment and refund request have different business meanings.

This is **at-least-once execution with idempotent effects**, not a distributed
exactly-once transaction across Kafka, MinIO and PostgreSQL. Lake files can exist
before DB commit; the manifest hides them from supported readers until completion.
DLQ delivery is at-least-once, so DLQ consumers must deduplicate the source key.

## Checkpoints and recovery

MinIO holds `checkpoints/raw/v1`, `checkpoints/dlq/v1` and
`checkpoints/valid-events/v1`. Raw retains Kafka offsets; downstream queries retain
raw-file offsets; valid-events also retains watermark/dedup state. The MinIO named
volume survives container replacement. Only a health heartbeat lives in `/tmp`.

| Failure | Behavior and recovery |
| --- | --- |
| Invalid record | Preserve raw, emit DLQ; valid processing continues |
| Duplicate record | Preserve both offsets; one logical sink event |
| Kafka unavailable | Bounded calls/retries eventually fail the query; restore broker and restart same checkpoints |
| MinIO unavailable | Archive/checkpoint/layer write fails; restore storage and preserve successful files/metadata |
| PostgreSQL unavailable | Staging or commit fails; no batch success; restore DB and replay pending raw input |
| Crash after staging | Recreate deterministic stage; partial rows never count as final events |
| Crash after lake writes | Reuse successful unpublished files; retry DB transaction and manifest |
| Crash after DB commit | Ledger survives; consume state, republish manifest, no duplicate metrics |
| Normal restart | Resume offsets and state under the same checkpoint UUID |

The supervisor exits if any query fails and stops all others. Compose deliberately
uses `restart: "no"` so failures remain visible. API health keeps its Phase 1 meaning;
stream health confirms active queries, not a freshness or latency SLA.

Only one active writer and a persistent namespace are supported. Never delete
individual checkpoint files, change shuffle partition count under existing state,
or reset one lake layer independently. A planned generation change can use a new
`STREAM_NAMESPACE`; database uniqueness survives, but a new namespace is not a
complete lake mirror of records already in PostgreSQL. Staging/attempt cleanup after
an abandoned generation is manual. Object-store rename is not filesystem-atomic:
readers must honor metadata. No automatic cleanup destroys recovery evidence.

## References

- [Spark 4.0.1 requirements](https://spark.apache.org/docs/4.0.1/)
- [Streaming semantics](https://spark.apache.org/docs/4.0.1/streaming/apis-on-dataframes-and-datasets.html)
- [Kafka integration](https://spark.apache.org/docs/4.0.1/streaming/structured-streaming-kafka-integration.html)
- [Hadoop AWS 3.4.1](https://hadoop.apache.org/docs/r3.4.1/hadoop-aws/tools/hadoop-aws/index.html)
