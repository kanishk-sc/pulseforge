# Local operations runbooks

These procedures target a local, disposable PulseForge Compose project. They never
delete volumes or Spark checkpoints. Confirm the project name and service list with
`docker compose ps` before stopping anything; a shared development stack may contain
other work. Restore each service in a `finally` step, even when an assertion fails.
None of these procedures is a production recovery plan.

## Start and inspect

```powershell
uv run python scripts/init_env.py
$env:OTEL_EXPORTER_OTLP_ENDPOINT='http://otel-collector:4318'
docker compose --profile observability up -d --build --wait api ops-exporter prometheus tempo otel-collector grafana
docker compose --profile product run --rm --no-deps product-migrate
docker compose exec prometheus promtool check config /etc/prometheus/prometheus.yml
docker compose exec prometheus promtool test rules /etc/prometheus/alert-tests.yml
Invoke-RestMethod 'http://127.0.0.1:9090/api/v1/targets'
```

Grafana: `http://127.0.0.1:3000`, user `admin`, password from the local `.env`.
Prometheus: `http://127.0.0.1:9090`. Tempo: `http://127.0.0.1:3200` or Grafana Explore.
All host ports are bound to `127.0.0.1`. To inspect an individual metric, use
`docker compose exec prometheus wget -qO- http://ops-exporter:9108/metrics` or
`Invoke-WebRequest http://127.0.0.1:8000/metrics`.

The default `docker compose up -d --build --wait` does not start optional telemetry.
To stop it without touching product/data volumes:

```powershell
docker compose --profile observability stop grafana prometheus tempo otel-collector ops-exporter
Remove-Item Env:OTEL_EXPORTER_OTLP_ENDPOINT -ErrorAction SilentlyContinue
docker compose up -d --force-recreate --wait api
```

## What each signal proves

`/health` proves only that the API process can answer. `/ready` probes PostgreSQL,
Kafka and object storage; it does not prove Spark is processing or analytics is fresh.
The API metrics show request-local traffic/errors/latency, not browser network latency.
`pulseforge_stream_last_progress_timestamp_seconds` is the driver's latest Spark
callback; a successful callback does not prove the PostgreSQL sink committed every
source event. `pulseforge_stream_source_offset` is Spark's raw-query end offset, not a
Kafka consumer-group lag. `pulseforge_warehouse_committed_rows` and the sink batch ledger
come from PostgreSQL and are replay-safe; Spark counters are diagnostic attempts and
reset on restart. `pulseforge_analytics_last_success_timestamp_seconds` describes the
last immutable publication, while its frozen source watermark describes its coverage.
Age alone is not a failure in an idle synthetic workload. Detector run records describe
finite attempts; business incidents remain separate from Prometheus alerts.

Missing time series are **unknown**. Check Prometheus `up`, scrape errors, container
status and logs before interpreting a blank panel. The optional producer is often
stopped; `up=0` there does not establish a failure. Quality results are absent for
successful publications created before Phase 5 or when the best-effort artifact write
failed. A last success can coexist with a newer failed build; inspect both.

## Triage and safe recovery

For API 5xx, inspect the `/ready` dependency map, route-template 5xx rate, API logs by
request ID/trace ID, then PostgreSQL availability. For cache errors, verify `X-Cache:
bypass` and a successful PostgreSQL response before restarting Redis. A Redis outage
should not change data or publication ID. For stream staleness, first establish active
producer deliveries; inspect query-specific progress, Kafka raw offsets, driver logs,
warehouse row/ledger counts and checkpoint namespace. Never delete a checkpoint to
"fix" lag. For publication lag, compare the warehouse ingestion watermark to the
frozen successful-build watermark, inspect latest build status and dbt quality artifact,
and rerun the finite pipeline with a **new** build key only when source data requires a
new generation. A failed build must not replace the previous success. For detector
failures, inspect the durable detector run, selected build, skip reason and source
evidence; reruns must remain incident-idempotent.

## Bounded failure drills

Use a disposable Compose project for destructive service interruption when possible.
Record `docker compose ps`, `/health`, `/ready`, a build-aware product response, and
`/api/v1/analytics/status` before/after. Never run `down -v`. The commands below stop
only a named service and always include restoration:

```powershell
try {
  docker compose stop redis
  Invoke-WebRequest 'http://127.0.0.1:8000/api/v1/revenue?limit=1000'
} finally { docker compose --profile product up -d --wait redis }

try {
  docker compose stop otel-collector
  Invoke-WebRequest 'http://127.0.0.1:8000/health'
} finally { docker compose --profile observability up -d --wait otel-collector }

try {
  docker compose stop postgres
  try { Invoke-WebRequest 'http://127.0.0.1:8000/ready' } catch { $_.Exception.Response.StatusCode.value__ }
  try { Invoke-WebRequest 'http://127.0.0.1:8000/api/v1/revenue?limit=1000' } catch { $_.Exception.Response.StatusCode.value__ }
} finally { docker compose up -d --wait postgres }
```

PostgreSQL loss should leave `/health` 200, make `/ready` 503 and return controlled
product 503; cached product points must not be served as if their build were current.
Collector or Grafana loss should not change business response codes. A streaming
restart uses `docker compose --profile streaming restart streaming`, then verifies
source positions and unique warehouse rows without clearing MinIO checkpoints. Existing
Phase 2 streaming integration tests exercise sink outage/recovery and duplicate replay.
Kafka/MinIO interruption is deferred on a shared dev stack unless a disposable project
with separate ports and volumes is available; do not stop unknown shared services.

## Local bounded load measurements

Use explicit windows and inspect the output's `scenario_condition_met`; a label such as
"warm" is not evidence that Redis actually hit. The harness limits requests, events,
concurrency and rate, records actual p50/p95/p99, statuses, cache headers and Docker
resource snapshots, and writes JSON. It does not set a pass/fail latency threshold.

```powershell
uv run python scripts/load_phase5.py --scenario warm --requests 200 --request-rate 20 --concurrency 4 --output docs/benchmarks/phase5-warm.json
uv run python scripts/load_phase5.py --scenario cold --requests 200 --request-rate 20 --concurrency 4 --output docs/benchmarks/phase5-cold.json
try {
  docker compose stop redis
  uv run python scripts/load_phase5.py --scenario fallback --requests 60 --request-rate 10 --concurrency 4 --output docs/benchmarks/phase5-fallback.json
} finally { docker compose --profile product up -d --wait redis }
docker compose --profile streaming up -d --build --wait streaming
uv run python scripts/load_phase5.py --scenario ingestion --events 30 --rate 5 --output docs/benchmarks/phase5-ingestion.json
```

The ingestion drain observation starts **after** producer completion and ends when
warehouse row count reaches the expected value. It includes two-second polling and is
not per-event processing latency. Event-time delay is not measured: synthetic historical
timestamps are workload data, not evidence of slow processing. A short local run is not
a capacity estimate, production SLA or availability guarantee.
