# Phase 1 verification

Executed on **2026-09-09** on Windows with Docker Desktop's Linux engine
(Docker Engine 29.5.2), Python 3.12.13 and uv 0.11.16. This is a functional
verification report, not a throughput or latency benchmark.

## Results

| Check | Command or procedure | Observed result |
| --- | --- | --- |
| Formatting | `uv run ruff format --check .` | Passed |
| Lint | `uv run ruff check .` | Passed |
| Contract export | `uv run python scripts/export_schema.py --check` | Matches Pydantic model |
| Full suite | `uv run pytest --run-integration` | 45 passed, including 5 live integration tests |
| Compose configuration | `docker compose config --quiet` | Passed |
| Container build/start | `docker compose up -d --build --wait --wait-timeout 180` | Built API/producer image; API, Kafka, PostgreSQL, MinIO healthy; bootstrap exited 0 |
| Bounded traffic | `docker compose run --rm -e ANOMALY_RATE=0 -e EVENTS_PER_SECOND=50 producer python -m pulseforge.producer --count 100 --scenario payment-spike` | 100 acknowledged records, exit 0, clean shutdown |
| Dependency outage | Stop PostgreSQL, query `/health` and `/ready`, restart PostgreSQL | Liveness 200, readiness 503, postgres down; Kafka/storage stayed up |

After restarting PostgreSQL, the full 45-test suite passed again, including readiness.
Foundation implementation committed as `c0b1884` (`feat: scaffold pulseforge platform`).

The live suite verifies a Kafka event at its acknowledged partition/offset, an S3
object write/read/delete, a PostgreSQL transaction, repeated initialization and
full API readiness. It does not substitute mocks for infrastructure. The non-integration
tests cover all eight generated event types, correlated journeys, invalid schema/data,
duplicate partition keys, deliberately malformed bytes, spike behavior, HTTP error
redaction, request IDs, producer failure paths, bounded retries and graceful stop.

## Failures found and fixed

- Initial bootstrap failed because boto3 clients do not implement the context-manager
  protocol. Replaced direct `with client` with `contextlib.closing` in bootstrap,
  readiness and the S3 integration test; rebuilt and reran the stack and suite.
- Kafka startup connection failures use aiokafka's `KafkaConnectionError`; explicitly
  included this exception in bounded startup retries and added retry/exhaustion tests.

## Known limits

- The current test dependencies emit two upstream deprecation warnings from Starlette's
  TestClient (httpx compatibility and an AnyIO alias). Tests pass; warnings are not hidden.
- GitHub Actions is configured, but a local run is not evidence of a completed hosted CI run.
- Phase 1 did not exercise Spark, dbt, Airflow, React, Redis, metrics servers, anomaly
  detection or AI. The Phase 2 section below records subsequent streaming evidence.
- No performance or AI evaluation results exist. A 100-event smoke run is not a benchmark.
- Local credentials are generated in `.env` and excluded from Git and the Docker context.

## Reproduce

```sh
uv sync --frozen
uv run python scripts/init_env.py
uv run ruff format --check .
uv run ruff check .
uv run python scripts/export_schema.py --check
docker compose config --quiet
docker compose up -d --build --wait --wait-timeout 180
uv run pytest --run-integration
```

For an outage drill, `docker compose stop postgres`, inspect `/health` and `/ready`,
then `docker compose start postgres` and confirm `/ready` returns 200 again.
Do not delete volumes to test a transient outage.

# Phase 2 verification

Execution date: **2026-09-09**. Windows host, Python 3.12.13, Docker Desktop Linux
engine 29.5.2. Spark uses a 1 GB driver heap, two local execution threads and a 3 GB
container limit. These are configuration values, not benchmark results.

## Verification scope

The phase is checked against both existing development data and a fresh Compose
project named `pulseforge-phase2-verification`, with new Kafka/PostgreSQL/MinIO
volumes. Original development volumes are preserved. The full command is:

```sh
docker compose --profile streaming up -d --build --wait --wait-timeout 240
uv run pytest --run-integration --run-streaming --junitxml=local-results.xml
```

The default non-streaming integration command remains available for the foundation.
CI now builds the streaming image and runs all integration tests, including controlled
restarts and PostgreSQL outage/recovery, without secrets or paid services.

## Hosted CI result

[Platform CI run 34361432528](https://github.com/kanishk-sc/pulseforge/actions/runs/34361432528)
completed successfully on implementation/test commit `aac0fee`:

- `python`: formatting, lint, schema export validation and **59 tests passed**.
- `compose-integration`: fresh Docker builds, healthy Spark stack and **14 integration
  tests passed**, including all seven streaming acceptance tests. Job logs confirm
  zero test failures; the other 59 cases were deliberately deselected in this job.

Milestone commits: `54e7049` (foundation cleanup and real producer coverage),
`1a85b53` (streaming implementation), `aac0fee` (acceptance tests and CI).
The concluding documentation commit changes no executable code.

## Executed local results

| Check | Actual result |
| --- | --- |
| Ruff formatting | 40 files already formatted |
| Ruff lint | All checks passed |
| Exported v1 JSON Schema | Matches shared Pydantic model |
| Non-integration suite | 59 passed; 14 integration tests deselected |
| Full fresh-volume suite | **73 passed, 0 failed, 0 errors, 0 skipped**, two upstream deprecation warnings |
| Compose build/start | Spark, API, Kafka, PostgreSQL and MinIO healthy; bootstrap exit 0 |
| Actual producer → Kafka → Spark | CLI-generated IDs validated by a real consumer and found in committed cleaned/curated data and PostgreSQL |
| Raw and DLQ | Malformed bytes retained byte-for-byte; malformed and missing-ID reasons reach DLQ; no invalid source tuple in warehouse |
| Duplicate event | Both Kafka offsets in raw, one cleaned record and one warehouse row |
| Minute aggregates | Event-type counts and decimal sums match independent SQL aggregation of unique event-time rows |
| Late event | Old record retained in raw, excluded from live warehouse, explicit watermark-drop diagnostic observed |
| Restart | All three checkpoint UUIDs retained; new event processed; replayed duplicate remains unique |
| PostgreSQL outage | Spark exits; failed batch has offsets but no checkpoint commit or DB ledger row; event absent until recovery |
| Recovery | Restart resumes the same checkpoint and commits the pending event once |
| Database replay | Repeating the same batch and the same input under another batch does not inflate rows or minute totals |

[Machine-readable results](phase-2-test-results.json) were extracted from the actual
JUnit XML, retaining all 73 case names and outcome counts. The temporary verification
containers were removed without deleting their volumes. The original development
stack was restored and all five long-running services returned healthy.

## Failures found during implementation

- A freshly created partitioned raw source initially had a schema mismatch when its
  first `ingest_date` partition appeared. Adding the explicit partition field fixed
  the source schema, and the fresh-volume startup exercises that path.
- Polling `lastProgress` could miss a stateful batch when Spark immediately committed
  an empty follow-up batch. A `StreamingQueryListener` now logs every progress event;
  the late-event test requires the explicit `watermark_dropped=1` diagnostic.
- Host PostgreSQL connections through `localhost` encountered Windows IPv6 fallback
  delays. Streaming's host default now matches Compose's IPv4 loopback binding.
- The actual-producer fixture initially assumed async metadata lookup and omitted
  random draws made by `next_record`. It now subscribes before inspecting partitions,
  captures end offsets, and derives expected IDs through the same public generator path.
- Array-valued event types and non-finite JSON numbers now produce safe validation
  failures rather than unexpected exceptions in executor code.

## Limits

- This is at-least-once multi-sink execution with idempotent effects, not a distributed
  exactly-once transaction. DLQ messages can repeat with the same source key.
- Very late events remain in raw and are reported by the watermark diagnostic, but
  can be absent from live cleaned/warehouse output. Offline reconciliation is future work.
- Cleaned/curated readers must honor commit manifests. The inspection CLI does so,
  but loads the small demo dataset into host memory.
- One active warehouse writer, single-node Kafka and local Spark are deliberate limits.
  File compaction and abandoned-generation staging cleanup are not automated.
- JVM warnings about native Hadoop libraries and object-store sync APIs are visible;
  they are not proof of HDFS-style atomic rename semantics on object storage.
- The two pre-existing Starlette/AnyIO deprecation warnings remain visible in Python tests.
- No throughput, latency-percentile or scalability benchmark was run or claimed.
