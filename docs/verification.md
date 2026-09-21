# Verification record

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


# Phase 3 review and re-verification — 2026-09-16

The existing Phase 3 implementation was reviewed on `phase-3-analytics`, then hardened
without changing the Phase 1 event contract or Phase 2 ingestion/checkpoint code.
The full regression below exercised implementation commit `e53eb5c`. The subsequent
registry-only fix `5d0d3a6` uses the identical MinIO image and passed a fresh foundation
verification. Historical September 9 results above remain historical evidence.

## Corrections and scope

- Materialize the event staging table once per full build. Dimensions and facts now
  share a committed source snapshot while Spark continues ingesting.
- Pass UTC explicitly to all hourly date_trunc expressions, including against a
  database configured for Asia/Kathmandu. The run-start hook alone did not establish
  a timezone on every worker connection.
- Return zero requests per order when there are orders but no requests; retain null
  when there is no order denominator.
- Reject multiple shipment creations for one order in a business-rule test, since
  contract v1 has no independent shipment identifier for unambiguous delay attribution.
- Fail the quality summary on empty, malformed, skipped, warning or unknown results.
- Mount the CLI dbt project read-only and write generated artifacts under /tmp in the
  container, removing the requirement to write to a Linux runner-owned checkout.
- Correct the minute metric grain (minute/event type, no region) and region spelling
  in the design documentation. Document partial model publication and serial builds.

## Exact executed commands and observed results

Commands ran in PowerShell. Output redirection to ignored diagnostic logs is omitted.

| Command | Observed result |
| --- | --- |
| `uv run ruff format --check .` | 46 files already formatted |
| `uv run ruff check .` | All checks passed |
| `uv run python scripts/export_schema.py --check` | Version 1 schema matches Pydantic |
| `uv run pytest -m "not integration" --basetemp=.pytest_cache/unit-tmp` | 68 passed; 18 integration cases deselected |
| `docker compose --profile analytics build analytics-dbt` | Exit 0 |
| `docker compose --profile analytics run --rm analytics-dbt parse` | Exit 0 |
| `docker compose --profile analytics run --rm analytics-dbt compile` | Exit 0 |
| `docker compose --profile analytics run --rm analytics-dbt build` | 14 models, 94 data tests, one hook: PASS=109, WARN=0, ERROR=0, SKIP=0 |
| `docker compose --profile analytics run --rm analytics-dbt test` | 94 tests and one hook: PASS=95, WARN=0, ERROR=0, SKIP=0 |
| `uv run pytest -m analytics --run-integration --run-analytics --basetemp=.pytest_cache/analytics-tmp` | 4 passed in 45.81 seconds; subsequent full run also covered the added zero-denominator fixture |
| `docker compose --profile streaming up -d --build --wait --wait-timeout 240` | Exit 0; healthy streaming stack |
| `uv run pytest --run-integration --run-streaming --run-analytics --basetemp=.pytest_cache/full-tmp --junitxml=phase3-review-results.xml` | **86 passed, 0 failures, 0 errors, 0 skipped**, 235.17 seconds; two existing upstream warnings |
| `docker compose --profile airflow build airflow` | Exit 0 |
| `docker compose --profile airflow run --rm --no-deps airflow python /opt/pulseforge/scripts/verify_airflow_dag.py` | Zero import errors; expected three-task linear DAG |
| `docker compose --profile airflow up -d --build --wait --wait-timeout 180 airflow` | Exit 0; healthy service |
| `docker compose exec airflow airflow dags test pulseforge_analytics 2026-09-16T15:00:00+00:00` | Exit 0; all three tasks and DAG successful; summary reports 94 pass, 15 success, zero failed |
| `docker compose --profile analytics run --rm --no-deps -e POSTGRES_PORT=1 analytics-dbt debug` | Expected nonzero exit on unavailable PostgreSQL endpoint; no service stopped |
| `docker compose config --quiet` | Exit 0 |
| `docker compose up -d --build --wait --wait-timeout 180` | Exit 0, including after the registry correction |
| `uv run pytest tests/test_integration.py --run-integration --basetemp=.pytest_cache/quay-tmp` | 5 passed in 26.32 seconds after registry correction |
| `git diff --check` | No whitespace errors |

[Machine-readable case results](phase-3-test-results.json) are extracted from the
actual full-run JUnit XML. The 86 cases comprise 68 non-integration tests, four analytics
acceptance tests and the existing 14 foundation/streaming integration tests.

The deterministic analytics fixture starts with 12 events and exact revenue USD 380,
two east payment attempts with one failure (rate 0.5), and two created shipments.
Replaying a UUID inserts zero events. An interleaved subsequent ingestion adds a late
order, a later shipment delay and a refund request in a region/hour without orders.
Downstream work using the already-captured staging table still sees four orders;
a full rebuild sees five, updates the existing shipment delay, keeps revenue at USD
380 and yields null for the request/order ratio without orders. A deliberately
corrupted analytics payment status makes the actual dbt test fail; a full rebuild
repairs it while the source retains exactly 15 events. All four contract regions are
represented after the late arrivals. The temporary test database is dropped afterward.

## Registry failure discovered by hosted CI

The first hosted run on `e53eb5c` failed before starting streaming tests because Docker
Hub denied access to `minio/minio`. The same pinned release is available through
[MinIO's documented Quay registry](https://github.com/minio/minio/blob/master/docs/docker/README.md).
The correction changes only the registry reference to
`quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z`. Locally, inspecting both tags returned
the identical image ID `sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e`.
No volumes, ports, settings, checkpoints or storage semantics changed.

[Hosted run 35117743583](https://github.com/kanishk-sc/pulseforge/actions/runs/35117743583)
completed successfully on `5d0d3a6`:

- Python: formatting, lint, schema verification and **68 passed** (3.02 seconds).
- Analytics: fresh image builds, dbt compilation, real DAG import and **4 passed**
  against populated PostgreSQL (52.08 seconds).
- Foundation/streaming: fresh stack startup and **14 passed** (92.53 seconds).
  The four analytics cases were intentionally skipped here and passed in their
  dedicated job; the 68 non-integration cases were deselected.

Implementation commits already present at review start were `9f658ea` (dbt),
`9c26881` (Airflow), `d0bbe71` (tests/CI), and `0b8c226` (initial verification docs).
Review corrections are `6a9aeed` (snapshot/metrics/acceptance), `e53eb5c` (quality
summary), and `5d0d3a6` (registry). The concluding documentation commit does not
change executable code. No merge to main was performed.

## Remaining operational limits

Models publish independently; a failing test does not atomically roll back the entire
analytics schema. Use successful build results and serialize manual builds with Airflow
for each target schema. Late events excluded by Spark's watermark remain raw-only and
cannot be recovered by dbt. Shipment cohort attribution requires one creation per order;
unmatched delays remain source evidence. Full source rescans, local SQLite Airflow
metadata, the shared development PostgreSQL role and single-node services remain
intentional development limits. No performance benchmark or Phase 4 feature is claimed.

# Phase 4 product verification — 2026-09-20

Phase 4 was implemented from merged `main` commit
`3594f2728362f656219c4099e960fe2cfb277db3`. Verification used the live Compose
PostgreSQL/Kafka/MinIO/Spark foundation, real dbt and Airflow images, Redis, FastAPI,
the production dashboard container and Chrome. This is functional correctness evidence,
not a throughput or availability benchmark.

## Executed results

| Check | Observed result |
| --- | --- |
| Ruff format/check | 74 files already formatted; all checks passed |
| Lockfile | `uv lock --check` resolved 69 packages without changes |
| Python non-Spark regression | **81 passed**, 22 integration cases deselected, two existing upstream deprecation warnings |
| Product/API-focused unit tests | **13 passed**, including detector thresholds, stale/empty/unavailable states, UTC bounds, decimal serialization, Redis bypass and incident filtering |
| Frontend | **4 Vitest tests passed**; TypeScript check and production Vite build exited 0 |
| Frontend dependency audit | `npm audit --audit-level=high`: zero vulnerabilities |
| Compose validation | Default and streaming/analytics/Airflow/product/observability profile combinations exited 0 |
| Product migrations | `0001` and `0002` applied; repeat runs report no pending migration |
| Airflow DAG import | Zero import errors; existing three-task linear graph preserved |
| dbt product runs | 14 models, 94 data tests and one hook: **PASS=109, WARN=0, ERROR=0, SKIP=0** |
| Live product integration | **3 passed**: exact PostgreSQL/API values, failed-build isolation, and exact incident/evidence response |
| Dashboard container | Production bundle built and served healthy at `http://127.0.0.1:5173` |
| Real browser | Overview, incident navigation/evidence, region filter, exact payment anomaly and analytics status visibly verified in Chrome |

The deterministic fixture committed 140 contract-valid payment records through the
existing replay-safe ingestion boundary. Six `ap-south` baseline hours each contained
20 successful attempts. The evaluation hour contained 20 failed attempts. The final
successful publication was:

- build `7a8dceed-7f77-44eb-b275-0f42a253c629`;
- dbt invocation `2ab6af20-1c6f-4ffb-a384-c06f68a584ef`;
- 370 committed source events; and
- published at `2026-09-20T22:50:42.596555Z`.

Detector version `1.0.0` created open incident
`4a1791b0-5222-4622-98bf-a8fe15aff83c` with observed rate `1.000000`, denominator
`20`, baseline `0.000000`, threshold `0.100000`, critical severity and exactly 20
source-event evidence rows. Both the incident evidence and its detector input are tied
to the same immutable build; SQL independently counted 20 rows in each set. Repeating
the evaluation created zero additional incidents.

The browser showed the same build and values. Filtering Payment Health to `ap-south`
displayed six healthy windows followed by 20 attempts, 20 failures and a 100.0% rate.
Selecting the finding from Overview navigated to the evidence panel and rendered all
20 UUIDs. Analytics Status showed the successful publication, source watermark and the
earlier failed build without claiming Spark liveness.

## Failure and recovery evidence

- The first publication attempt used an incorrect flat `analytics` schema. dbt itself
  passed all 109 nodes, publication rolled back, and build
  `901c60ee-495b-407d-8433-62b4daa65fae` was retained as failed. Correcting the names
  to `analytics_marts` and `analytics_core` produced a successful build; the API never
  served the failed attempt.
- The first persisted incident attempt exposed use of `Connection.executemany`, which
  Psycopg 3 provides on a cursor. The incident transaction rolled back. Switching to a
  cursor produced one incident plus 20 evidence rows; a repeat produced zero. The
  pipeline also now resumes an already-published build key by rerunning only detection,
  so this exact post-publication failure is recoverable on an Airflow retry.
- Incident list/detail initially selected every database column, and strict response
  models rejected internal uniqueness fields. Explicit public projections fixed both
  routes; live list/detail calls returned 200.
- Browser testing found that Overview incident selection loaded detail without changing
  views. The action now navigates to Incidents; a component test and the repeated real
  browser flow verify it.
- Evidence initially came from mutable dbt facts after publication. Migration `0002`
  adds a build-tagged evidence snapshot inside the repeatable-read publication, and all
  detectors now read that immutable projection.
- Vitest 3 initially produced a dependency audit finding. Upgrading to Vitest 5.0.1
  retained passing tests and reduced `npm audit` to zero vulnerabilities.
- Host PostgreSQL access through `localhost` stalled on this Windows configuration;
  product integration defaults now use `127.0.0.1` and a five-second connect timeout.
- The first Phase 4 hosted product job sourced `.env` in Bash; the valid unquoted value
  `10 minutes` was interpreted as a command. The application pipeline had already
  succeeded. The test now loads the project's typed settings directly, avoiding shell
  parsing of environment files; the exact local integration rerun passed 3/3.
- The complete isolated Python run reached the seven existing Spark transform tests but
  the local gateway did not start because this host resolves Java 8 and Spark 4 requires
  a newer JVM. The run was interrupted after a bounded wait. All other 81 cases passed.
  The unchanged Spark tests passed in Phase 3 hosted CI, and Phase 4 CI reruns them on
  Ubuntu while adding a separate real product-integration job.

## Reproduce the product acceptance

```sh
uv sync --frozen
uv run python scripts/init_env.py
docker compose up -d --wait --wait-timeout 180 postgres
docker compose --profile product run --rm --build product-migrate
uv run python scripts/seed_product_acceptance.py
docker compose --profile airflow build airflow
docker compose --profile airflow run --rm --no-deps airflow python -m pulseforge.product.cli pipeline --build-key local-product-build --project-dir /opt/pulseforge/analytics --profiles-dir /opt/pulseforge/analytics --detector-now <detector_now-from-seed-output>
docker compose --profile product up -d --build --wait --wait-timeout 180 redis api dashboard
uv run pytest tests/test_product_integration.py --run-integration --run-product
```

In PowerShell, copy the printed `detector_now` value into the final pipeline command.
On Bash, it can be parsed from the seed JSON as the CI workflow does. Do not reuse a
successful build key. No hosted Phase 4 result is claimed until the branch is pushed and
the new workflow completes.
