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
- At the time of this Phase 1 report, Spark, dbt, Airflow, React, Redis, metrics and AI
  execution were not claimed. Later sections record the subsequently executed phases.
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

## Phase 2 streaming verification

Executed on **2026-09-18** on Windows with Docker Desktop's Linux engine, Spark 3.5.9,
Python 3.12.13 and an isolated Kafka/PostgreSQL/MinIO stack. This is a correctness
smoke test, not a throughput benchmark.

| Check | Observed result |
| --- | --- |
| Formatting and lint | Ruff format check and lint passed |
| Spark transformations | 7 Spark tests passed, including non-UTF-8 raw-byte preservation |
| Full non-infrastructure suite | 47 tests passed; 6 infrastructure tests deselected |
| Container build | Pinned Spark image and Kafka/PostgreSQL/S3 connector resolution succeeded |
| Initial bounded stream | 200 Kafka inputs produced 179 unique accepted rows and 15 dead-letter records |
| Event sink integrity | 179 rows, 179 distinct IDs, zero persisted duplicates |
| Minute metrics | 179 events, 39 payment attempts, 22 failures and USD 4,683.30 successful revenue |
| Lake output | Raw and cleaned Parquet objects were written to isolated MinIO storage |
| Automated all-sink path | Opt-in integration test delivered a valid event to PostgreSQL/cleaned MinIO and an invalid event to raw MinIO/dead-letter Kafka |
| Deterministic replay | Replayed the same 200 IDs; event and metric counts remained unchanged; raw evidence and DLQ records increased |
| Checkpoint restart | Forced a Spark container recreation; five queries resumed without errors and counts remained unchanged |

The failure/recovery path also exposed a real warehouse defect: staging represented UUID
and JSON values as text, while the final table required `uuid` and `jsonb`. The merge now
casts those values explicitly. Restarting from the unchanged checkpoint retried the batch,
loaded all 179 valid events and left no duplicate event IDs.

Current limits: the all-sink delivery path is automated, but deliberate sink outages
and restart recovery remain manual smoke procedures; rejected records are intentionally replayable and therefore at-least-once in the
dead-letter topic; the single local Spark driver is not a production cluster; curated
business models and compaction belong to Phase 3. Hosted GitHub Actions status is not
claimed because this verification ran locally.

## Phase 3 dbt verification

Executed on **2026-09-18** against the same populated isolated PostgreSQL warehouse.
dbt Core 1.12.5 with dbt-postgres 1.11.0 built 14 models and ran 43 data tests.
The result was **55 pass, 2 warnings, 0 errors, 0 skips** across 57 operations.

The warnings are evidence rather than ignored failures: deliberate upstream corruption
left five payment attempts and three shipment events without their order-created event.
Relationship tests report those rows at warning severity, while
`mart_data_quality_hourly` persists their counts by hour and region. The modeled revenue
reconciled to the stream aggregate at USD 4,683.30 across 17 successful payments.

## Phase 3 Airflow verification

Executed on **2026-09-18** against the populated isolated stack with Apache Airflow
3.3.2 and dbt Core 1.12.5. The custom Airflow image built successfully and `pip check`
reported no broken requirements. Database migration and DAG reserialization succeeded;
`airflow dags list-import-errors --output json` returned an empty list and all three
DAGs were registered.

| DAG | Observed execution result |
| --- | --- |
| `pulseforge_analytics_pipeline` | Four tasks succeeded: source freshness, staging/dimension/fact build, mart build and 43 dbt tests (41 pass, 2 expected warnings) |
| `pulseforge_data_quality_report` | Two tasks succeeded and wrote `artifacts/quality/20260918T202124Z.json` from dbt's real run-results artifact (41 pass, 2 warnings, no failures) |
| `pulseforge_lake_retention` | Task succeeded in the default dry-run mode with 0 expired candidates and 0 deletions |

The DAG runs used actual PostgreSQL and MinIO services. The retention result proves the
safe no-delete default and execution path; it is not evidence that deletion of expired
objects has been exercised. Airflow standalone uses SQLite locally and is not presented
as a highly available production deployment.

## Product, observability and deployment-target verification

Executed on **2026-09-18/19** against the populated isolated stack. The warehouse
contained the same 17 successful payments and USD 4,683.30 revenue reconciled above.

| Check | Observed result |
| --- | --- |
| Python quality | Ruff format/check and schema export passed; 50 non-integration tests passed, 6 live tests deselected |
| Frontend quality | npm audit reported 0 vulnerabilities; TypeScript check and Vite production build passed |
| Analytics API | Overview returned actual revenue/payment/shipment/refund/operations/quality marts; repeated request reported a Redis cache hit |
| Cache outage | With Redis stopped and an uncached time window, overview still returned HTTP 200 from PostgreSQL; Redis was restarted |
| Dashboard | Production Nginx image served the React bundle and proxied `/api/metrics/overview` to the real API |
| Metrics | Prometheus health passed and `up{job="pulseforge-api"}` returned 1 after a real scrape |
| Grafana | Provisioned container health returned database `ok` on Grafana 12.1.1 |
| Compose | All profiles parsed successfully; isolated API, dashboard, Redis, Prometheus and Grafana started |
| Terraform | Terraform 1.13.5 initialized AWS/random providers and `terraform validate` returned success |

The frontend build reports a 577.35 kB minified JavaScript chunk (172.38 kB gzip), so
route/chart code splitting is worthwhile future work; no Core Web Vitals or load claim
is inferred from a successful build. Terraform validation is syntax/provider-schema
evidence only. No AWS plan or apply ran, and no public deployment is claimed.
