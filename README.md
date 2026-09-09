# PulseForge

**A real-time data and AI operations platform for synthetic commerce and logistics.**

PulseForge explores how an engineering team can trace business events from ingestion
to reliable operational decisions: preserve the original event, validate its contract,
process it once at the sink, model the business, detect explainable anomalies, and
show the evidence behind an incident.

**Current milestone: Phase 2 — streaming platform, verified locally and in CI.**
Kafka ingestion, Spark Structured Streaming, raw/cleaned/curated Parquet, a dead-letter
pipeline and an idempotent PostgreSQL sink are implemented. dbt, the dashboard,
anomaly detection and the AI assistant remain future work. All generated data is synthetic.

See the [implementation checklist](docs/architecture/implementation-plan.md) and
[actual verification record](docs/verification.md).

## Business problem

A payment provider can fail in one region while aggregate revenue still looks normal.
Shipment delays and refunds can follow later. Teams need fresh metrics, trustworthy
data and a reproducible path from an alert back to its source events. PulseForge's
target workflow connects those stages without making an LLM responsible for detection.

## Architecture

Solid arrows describe implemented paths. Dashed arrows describe future phases.

```mermaid
flowchart LR
    P[Python synthetic producer] --> K[Kafka: versioned event topic]
    B[Idempotent bootstrap] --> K
    B --> L[MinIO / S3-compatible lake]
    A[FastAPI liveness and readiness] --> K
    A --> L
    A --> W[(PostgreSQL)]
    K --> S[Spark archive query]
    S --> RAW[MinIO raw Parquet]
    RAW --> V[Spark validation and event-time deduplication]
    V --> D[Kafka dead-letter topic]
    V --> C[MinIO cleaned / curated Parquet]
    V --> ST[PostgreSQL JDBC staging]
    ST --> W
    W --> M[Minute operational aggregates]
    W -. Phase 3 .-> DBT[dbt facts / dimensions / marts]
    AF[Airflow batch orchestration] -.-> DBT
    DBT -. Phase 4 .-> API[Metrics and incident APIs]
    API -.-> UI[React operations dashboard]
    RAG[Phase 6: evidence-based assistant] -.-> API
    RAG -.-> V[(pgvector runbooks)]
```

| Layer | Technology | Purpose and status |
| --- | --- | --- |
| Contracts / producer | Python, Pydantic, aiokafka | Implemented: versioned validation, journeys, fault injection, confirmed delivery |
| Event backbone | Kafka in KRaft mode | Implemented: three partitions, event and dead-letter topics, seven-day retention |
| Database | PostgreSQL, SQLAlchemy, asyncpg, psycopg, JDBC | Unique stream events, transactional batch ledger and minute aggregates |
| Lake | MinIO, S3A, Parquet | Immutable raw evidence; committed cleaned/curated batches |
| API | FastAPI | Implemented: liveness, dependency readiness, OpenAPI, request IDs, JSON logs |
| Streaming | Spark 4.0.1, PySpark, Kafka connector | Event-time deduplication, DLQ, checkpoint recovery and JDBC staging |
| Modeling | dbt, Airflow | Phase 3 |
| Product | React, TypeScript, Redis | Phase 4 |
| Telemetry | Prometheus, Grafana, OpenTelemetry | Phase 5 |
| Assistant | pgvector, provider abstraction | Phase 6, optional paid provider, offline support |
| Deployment | Kubernetes, Terraform AWS | Phase 7, gated on local verification |

## Local setup

Prerequisites: Docker Desktop with Linux containers, Docker Compose v2, Git and
[uv](https://docs.astral.sh/uv/getting-started/installation/). Allow approximately
4 GB of Docker memory for the foundation; the Spark/Airflow phases will require more.
Commands work in PowerShell and Bash unless noted.

```sh
git clone https://github.com/kanishk-sc/pulseforge.git
cd pulseforge
uv sync --frozen
uv run python scripts/init_env.py
docker compose up -d --build --wait --wait-timeout 180
```

The environment script generates random credentials in ignored `.env`, and preserves
an existing file. Alternatively copy `.env.example` to `.env` and fill in both password
values yourself. Blank credentials deliberately fail Compose validation. Never commit
`.env`. All published ports bind to localhost; this is a development stack without TLS
or application authentication, and must not be exposed to the Internet.

| Service | Local address |
| --- | --- |
| API documentation | http://localhost:8000/docs |
| API liveness | http://localhost:8000/health |
| Dependency readiness | http://localhost:8000/ready |
| MinIO console | http://localhost:9001 (credentials from `.env`) |
| S3 endpoint | http://localhost:9000 |
| Kafka bootstrap | localhost:9092 |
| PostgreSQL | localhost:5432 |

Initialization creates `commerce.events.v1`, `commerce.dead-letter.v1`, and the
`pulseforge` bucket. It is safe to rerun. Starting streaming also runs the idempotent
`warehouse.sql` schema initialization; final dimensional modeling belongs to Phase 3.

### Generate and inspect events

Start continuous traffic explicitly so the default stack does not fill Kafka unattended:

```sh
docker compose --profile traffic up -d producer
docker compose logs -f producer
docker compose stop producer
```

Produce a bounded scenario, then inspect records:

```sh
docker compose run --rm producer python -m pulseforge.producer --count 100 --scenario payment-spike
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic commerce.events.v1 --from-beginning --max-messages 5
```

Scenarios: `normal`, `payment-spike`, `refund-spike`, `shipment-delays`, `large-orders`.
Set `EVENTS_PER_SECOND`, `ANOMALY_RATE` and `GENERATOR_SEED` in `.env`. To send only
schema-valid events for a controlled experiment:

```sh
docker compose run --rm -e ANOMALY_RATE=0 producer python -m pulseforge.producer --count 100
```

Events cover `customer_login`, `order_created`, `payment_processed`, `payment_failed`,
`shipment_created`, `shipment_delayed`, `refund_requested`, and `inventory_updated`.
A journey shares customer, order, product and region; failed payments do not ship.
Amounts use decimal USD values, serialized as strings to avoid binary floating-point
loss. A fixed seed reproduces choices and IDs when the clock is fixed. A producer
restart with the same seed intentionally repeats IDs, a useful replay/deduplication
test; use a different seed for a new independent simulation.

`ANOMALY_RATE` controls transport corruption: duplicate records, malformed JSON and
missing event IDs. Business spikes are separate scenario parameters. Invalid records
enter the event topic intentionally; Spark archives them and routes them to the
dead-letter topic. The generator validates good events before
serializing them, then corrupts selected bytes deliberately.

### Start and inspect streaming

Spark runs behind a profile so the lightweight API/foundation remains independent.
Allow Docker approximately 6 GB of memory for the combined platform. The streaming
container is capped at 3 GB and uses `local[2]`, not a pretend distributed cluster.
The first image build downloads Spark and its pinned JVM dependencies.

```sh
docker compose --profile streaming up -d --build --wait --wait-timeout 240
docker compose logs -f streaming
docker compose run --rm -e ANOMALY_RATE=0.25 producer python -m pulseforge.producer --count 100
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic commerce.dead-letter.v1 --from-beginning --max-messages 5
uv run python -m pulseforge.streaming.inspect --layer raw --limit 3
uv run python -m pulseforge.streaming.inspect --layer cleaned --limit 3
uv run python -m pulseforge.streaming.inspect --layer curated --limit 3
docker compose exec postgres psql -U pulseforge -d pulseforge -c "SELECT event_type,count(*) FROM stream_events GROUP BY 1;"
docker compose exec postgres psql -U pulseforge -d pulseforge -c "SELECT * FROM stream_metrics_minute ORDER BY window_start DESC LIMIT 10;"
```

The SQL examples use the default local username/database; substitute values from `.env`
if changed. In MinIO's console, browse the `pulseforge` bucket: `raw/v1`, `cleaned/v1`,
`curated/v1`, `commits/v1`, and `checkpoints/`. The host inspection command uses PyArrow
from the development dependencies and reads **committed files only**. It loads the
small demo dataset into memory; it is not a production query engine. Do not recursively
scan arbitrary lake files as though in-progress attempts were committed data.

To restart safely without discarding offsets or deduplication state:

```sh
docker compose stop streaming
docker compose --profile streaming up -d --wait --wait-timeout 180 streaming
```

All three checkpoints live in MinIO. Retain the checkpoint and lake metadata together.
On a correctness-critical sink failure Spark exits; restore the dependency and use the
same restart command. It does not auto-restart forever and conceal a failed sink.

The ten-minute watermark bounds the live deduplication state. Very late records can be
excluded from live cleaned/warehouse output; their bytes remain in raw and Spark logs
the dropped count. Planned offline reconciliation must use the raw archive. Do not
delete a checkpoint to repair a transient outage. Read [failure semantics](docs/architecture/phase-2-design.md)
before changing namespaces or planning a backfill.

### Example API usage

```sh
curl http://localhost:8000/health
curl -H "X-Request-ID: demo-001" http://localhost:8000/ready
```

In PowerShell, use `curl.exe` or `Invoke-RestMethod`. `/health` reports process liveness;
`/ready` checks a real PostgreSQL query, the Kafka event topic, and the S3 bucket. It
returns HTTP 503 when a dependency is unavailable and never returns raw connection
errors. Metrics and incident endpoints will arrive with populated data in Phase 4.

### Development and tests

```sh
uv run ruff format --check .
uv run ruff check .
uv run pytest -m "not integration"
uv run pytest -m integration --run-integration
uv run pytest --run-integration --run-streaming
```

The second pytest command requires Compose. It verifies Kafka delivery/readback,
S3 write/read/delete, PostgreSQL transactions, API readiness and repeated bootstrap.
The streaming flag additionally exercises actual CLI producer → Spark → lake/warehouse,
DLQ payload fidelity, duplicates, minute totals, watermark drops, restart and database
outage/recovery. These tests intentionally stop/restart local services: run against a
development stack, without another traffic generator. Integration tests skip by default.
CI builds the Spark image and runs the full integration suite without private secrets.
Frontend and dbt CI will be added with their implementations.

For host API development, stop the container API first, then run:

```sh
docker compose stop api
uv run uvicorn pulseforge.api:app --reload --no-access-log
```

`make setup`, `make up`, `make traffic`, `make lint`, `make test` and `make integration`
are optional shortcuts when Make is installed. The explicit commands above work on
Windows without Make. `docker compose down` stops the stack and preserves its data.

## Data models and event flow

The current source contract is `src/pulseforge/events.py`, exported as JSON Schema at
`schemas/commerce-event.v1.json`. Runtime validation additionally enforces timezone and
timestamp bounds, per-type required fields and inventory quantity rules. JSON Schema
alone cannot express all of those checks; consumers must use the runtime validator.

The planned warehouse uses event-grain staging with `event_id` uniqueness, dimensions
for customer/product/region and business facts for orders/payments/shipments/refunds.
Facts will retain source event references. dbt marts will calculate hourly revenue,
payment failure rates, shipment performance, refunds and operational health, with
explicit denominators and late-arrival handling. See [design decisions](docs/architecture/architecture.md).

## Observability and AI

API, producer and streaming application logs are JSON; Spark's JVM logs retain their
native diagnostic format. API responses include a validated or generated request ID.
Producer logs distinguish acknowledged events from a failed delivery. Streaming logs
report batch input/insert counts and watermark drops. The Spark container health check
reports live query threads, not an end-to-end latency or freshness guarantee.
Dependency failures are visible through readiness, independent of API liveness.
Phase 5 adds metrics and traces after the pipeline has meaningful measurements.

The planned assistant retrieves runbooks, incident evidence and recent metrics before
responding. Statistical detection remains outside the LLM. The main platform will
remain functional without an LLM key; offline output will be labeled as an evidence
summary. Evaluation will distinguish measured retrieval/latency metrics from human
judgments of usefulness. No AI accuracy results exist yet.

## Screenshots and benchmarks

No dashboard screenshot is included because the dashboard is not implemented yet.
No load-test performance numbers are claimed. The foundation verification record is
a functional test report, not a benchmark. Later benchmark reports will identify the
environment, concurrency, throughput, p50/p95/p99 and error rate.

## Engineering decisions and next milestones

- Kafka decouples event ingestion from downstream outages and supports replay.
- KRaft and one broker keep local setup small; this is not a highly available deployment.
- Producer idempotence handles broker retries within a session; durable sink deduplication
  is still required across restarts and for intentionally duplicated application events.
- Readiness probes fail closed, with bounded calls; liveness stays independent.
- One Python package shares contracts across separately runnable services. Separate
  Python projects would add packaging overhead before independent release cycles exist.
- No placeholder infrastructure, empty application folders, fake charts or fabricated scores.

Next is Phase 3: dbt facts/dimensions/marts and Airflow batch orchestration.
Subsequent phases add operational APIs, the dashboard,
observability and evidence-based AI. Kubernetes and AWS Terraform follow only after
the local application works; no paid infrastructure is created automatically.
