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

# Phase 3 verification

Execution date: **2026-09-09**. Windows host, Python 3.12.13, uv 0.11.16,
Docker Desktop Linux engine 29.5.2, PostgreSQL 16.9, dbt Core 1.12.2,
dbt-postgres 1.11.0 and Apache Airflow 3.3.0. This is functional correctness
evidence, not a performance benchmark.

## Verification scope

Phase 3 was exercised against the live Phase 2 warehouse and in a UUID-named,
disposable database created inside the real PostgreSQL service. The deterministic
fixture initialized the exact Phase 2 schema, committed 12 accepted events through
the existing transactional sink, attempted an event-ID replay, ran dbt twice and
dropped the database afterward.

The full regression command ran every foundation, streaming and analytics case while
the streaming service was healthy:

```sh
docker compose --profile streaming up -d --build --wait --wait-timeout 240
uv run pytest --run-integration --run-streaming --run-analytics --basetemp=.pytest_cache/full-tmp --junitxml=phase-3-results.xml
```

## Executed local results

| Check | Actual result |
| --- | --- |
| Ruff format/check | 46 Python files already formatted; all lint checks passed |
| Non-integration regression | 62 passed, 17 integration tests deselected |
| dbt parse and compile | Both exited 0 against the configured PostgreSQL warehouse |
| dbt build | 14 models and 93 data tests; **PASS=108, WARN=0, ERROR=0, SKIP=0, TOTAL=108** |
| Explicit dbt test | 93 data tests plus the project hook; **PASS=94, WARN=0, ERROR=0, SKIP=0, TOTAL=94** |
| Analytics acceptance | 3 passed in a disposable real database; exact dimension, fact, mart, lineage, replay and rerun assertions passed |
| Full local regression | **79 passed, 0 failed, 0 errors, 0 skipped** in 217.93 seconds; two known upstream deprecation warnings |
| Default Compose compatibility | `docker compose up -d --build --wait --wait-timeout 180` exited 0; API, Kafka, PostgreSQL and MinIO healthy; bootstrap exited 0 |
| Airflow DAG import | Zero import errors; exactly `verify_warehouse`, `dbt_build`, `quality_summary` with the required linear dependencies |
| Airflow service | Standalone service healthy; metadata database, scheduler, triggerer and DAG processor health checks healthy |
| Real Airflow run | `airflow dags test pulseforge_analytics 2026-09-09T18:00:00+00:00` exited 0; all three tasks and the DAG run succeeded |
| Airflow-run quality summary | `result_count=108`, statuses `pass=93` and `success=15`, failed count 0 |
| Failure-domain separation | The streaming and Airflow containers were concurrently healthy after the DAG; 185 Phase 2 source events and existing analytics relations remained queryable |

The deterministic expected values included four orders, four payment attempts, two
created shipments, one refund request, four customers, four products and four regions.
Successful-payment revenue totaled USD 380.00. The 10:00 east payment cohort contained
two attempts and one failure for a 0.5 failure rate. One shipment was attributed exactly
one delay; the other was not delayed. Repeating the source UUID committed zero new source
rows, and the second dbt build retained stable source, fact and revenue totals.

Milestone implementation commits are `9f658ea` (dbt models), `9c26881` (Airflow
orchestration) and `d0bbe71` (acceptance tests and CI). The concluding documentation
commit changes no executable code.

## Failure behavior verified by construction and execution

- PostgreSQL availability is checked before transformation. `dbt debug` and `dbt build`
  return nonzero on connection failure; Airflow retries twice with a one-minute delay,
  then leaves the DAG failed and visible.
- A model error or dbt data-test failure makes `dbt_build` fail. Airflow's default
  all-success dependency prevents the summary task from masking that failure.
- The quality-summary program fails closed when results are missing, malformed or
  contain failure/error statuses; unit tests cover successful and failed result sets.
- Facts merge by source event UUID and dbt never writes the Phase 2 `public` sources.
  The populated integration test's replay and second build prove stable row counts.
- The DAG contains no Spark, streaming or checkpoint operation. The real DAG completed
  while Spark continued from its existing checkpoint, demonstrating independent local
  operation rather than scheduler control of the stream.

## Failures found during implementation

- dbt 1.12 rejects global project/profile arguments placed before the subcommand. The
  container now supplies the profiles directory through its environment and invokes
  subcommands in the analytics working directory.
- A read-only analytics bind prevented dbt from writing generated target artifacts.
  The project bind is writable while generated target/log/package paths remain ignored.
- Airflow 3.3's public `DagBag` location and constructor differ from older examples.
  The verifier now uses the current public import and constructor and executes inside
  the pinned image.
- Airflow could not initialize a root-owned named volume as its non-root runtime user.
  The image now creates and owns its state directory before dropping privileges.
- Windows denied access to pytest's default temporary directory after Docker-mounted
  integration work. The successful local reruns supplied the documented workspace-local,
  ignored `--basetemp`; no assertion or service behavior was changed.

## Hosted CI status and limits

- The workflow now has a separate `analytics-integration` job that builds both images,
  compiles dbt, imports the real DAG and runs the deterministic PostgreSQL acceptance
  test. It preserves the prior Python and streaming jobs.
- No hosted Phase 3 run is claimed yet. Local workflow inspection and execution are not
  evidence that GitHub Actions completed the new commit.
- dbt deliberately rescans relevant accepted events for correct replay and late-arrival
  behavior at this scale. A measured high-volume workload may justify a source-change
  strategy later, but must preserve those semantics.
- The streaming watermark can exclude a very late event before it reaches PostgreSQL.
  dbt cannot recover that raw-only evidence; offline reconciliation remains future work.
- Local dbt and Phase 2 services share one generated PostgreSQL development role.
  Separate least-privilege source-reader and schema-owner roles are required before
  production exposure.
- Airflow's local SQLite metadata and simple auth are development choices. The generated
  password has no checked-in default, and port 8080 binds to loopback only.
- No dashboard, anomaly detector, incident API, telemetry system, AI assistant, cloud
  resource, throughput benchmark or scalability claim is part of Phase 3.

## Reproduce Phase 3

```sh
uv sync --frozen
uv run python scripts/init_env.py
uv run ruff format --check .
uv run ruff check .
uv run pytest -m "not integration" --basetemp=.pytest_cache/unit-tmp
docker compose up -d --build --wait --wait-timeout 180
docker compose --profile analytics build analytics-dbt
docker compose --profile analytics run --rm analytics-dbt compile
docker compose --profile analytics run --rm analytics-dbt build
docker compose --profile airflow build airflow
docker compose --profile airflow run --rm --no-deps airflow python /opt/pulseforge/scripts/verify_airflow_dag.py
uv run pytest -m analytics --run-integration --run-analytics --basetemp=.pytest_cache/analytics-tmp
```

For the full Phase 2 plus Phase 3 regression, start the streaming profile and use the
full regression command above. The tests intentionally stop and restart local services;
run them only against a development stack without an independent producer.
