# Single-host deployment component contract

All services share the private Compose bridge on one x86_64 EC2 host (or a
separate local Compose project). Image tags `pulseforge-*:SHA` mean the complete
reviewed Git commit, verified against a local image-ID manifest. No image is
published by this phase. The release override makes all host ports loopback;
intra-Compose service names and ports below do not imply public access. `.env`
is operator-injected, untracked and not baked into images. See
[`docker-compose.yml`](../../docker-compose.yml) and the
[release override](../../infra/docker/compose.release.yml) for exact commands,
health checks and environment variables.

| Component / image and entrypoint | Configuration, access, persistence and readiness | Sizing, lifecycle and scaling |
| --- | --- | --- |
| Producer / `pulseforge-python:SHA`, `python -m pulseforge.producer` | Kafka `kafka:29092`, bootstrap-complete prerequisite; no volume; delivery confirmations in logs | 512 MiB, finite/explicit run, 45s stop grace, **one** producer for the documented fixture; event identity contract controls duplicate effects |
| Kafka / `apache/kafka@sha256:4ceccc…`, bundled KRaft | Internal PLAINTEXT 29092, loopback external 19092; `kafka-data`; topic-list health | 2 GiB, 60s stop grace, one broker/controller; no replication; configured retention bounds replay |
| Spark / `pulseforge-streaming:SHA`, `python -m pulseforge.streaming` | Depends on healthy PG and bootstrap; Kafka, MinIO, PG via bridge; checkpoint objects in MinIO; live-query health | 3 GiB, 120s graceful stop, manual restart only, **one checkpoint owner**; never Airflow-managed |
| PostgreSQL/pgvector / `pgvector/pgvector@sha256:ccc6…`, stock server | `POSTGRES_*` from `.env`; loopback 15432, bridge 5432; `postgres-data`; `pg_isready` | 2 GiB, 60s stop grace, single durable instance; no failover; warehouse/event/ledger/publication/incident/corpus authority |
| MinIO / `pulseforge-minio:SHA`, checksum-verified server | Root credentials from `.env`; bridge 9000, loopback 19000/19001; `minio-data`; live health | 2 GiB, 60s stop grace, one server; lake data/commits/checkpoints; inherited root runtime is a documented hardening gap |
| Bootstrap / `pulseforge-python:SHA`, `python -m pulseforge.bootstrap` | Waits for Kafka/MinIO health; creates topics/buckets idempotently | 512 MiB finite job, restart `no`; rerun before streaming after recovery |
| dbt / `pulseforge-dbt:SHA`, `dbt` | PG bridge, read-only analytics source bind, target/logs ephemeral; PG health | 768 MiB, finite job, one writer; direct `dbt build` must not overlap a publication pipeline |
| Product migration/detector / `pulseforge-python:SHA`, CLI `migrate`/`detect` | PG bridge; migration transaction lock; detector reads latest successful build and persists incidents | 512 MiB each, finite, restart `no`; repeatable migration; detector safe/idempotent for re-evaluation |
| Airflow / `pulseforge-airflow:SHA`, `standalone` | PG bridge, `airflow-state`, loopback 18080; HTTP version health | 1536 MiB, one scheduler, finite DAG only; no Spark control; local simple-auth manager is not public-access security |
| API / `pulseforge-python:SHA`, Uvicorn | PG/Kafka/MinIO/Redis bridge; loopback 18000; read-only `model-cache` and `.env`; `/health` live, `/ready` dependency-sensitive | 1536 MiB, 30s stop grace, one instance; no external auth/TLS; publication/corpus semantics unchanged |
| Dashboard / `pulseforge-dashboard:SHA`, nginx | Reverse-proxies API by service DNS; loopback 15173; `/` health | 256 MiB, restart unless stopped, one instance; browser has no application login |
| Redis / `redis@sha256:02f2…`, AOF server | Bridge only, `redis-data`, ping health | 256 MiB; disposable build-aware cache, not business authority; failures fall back to PG |
| Assistant setup / `pulseforge-python:SHA`, `download-model`, `migrate`, `ingest` | Explicit profile only; writable model cache for download, read-only for ingestion/API; PG assistant schema and versioned corpus; provider disabled | Model/ingest 1536 MiB, migration 512 MiB; finite single jobs; no implicit download or provider call; vectors rebuildable |
| Metrics / `ops-exporter` Python, Prometheus, Grafana | Optional `observability`; bridge scraping; loopback 19090/13000; PG exporter; `prometheus-data`/`grafana-data` | Exporter 256 MiB; Prometheus 48h/512MB TSDB; best-effort diagnostics, no data-path dependency |
| Tracing / OpenTelemetry collector and Tempo | Optional `observability`; loopback 14318/13200; Tempo `tempo-data`; no public ingestion | 256/512 MiB; failure isolated; Tempo inherited root runtime and local trace retention make this **non-hardened**, not a production observability target |

Services without an explicit Compose memory limit retain Docker's host-level
contention risk; the 32-GiB sample host is an estimate, not measured capacity.
Named Docker volumes must reside on the protected data EBS volume before a cloud
start. The data EBS volume and the S3 backup bucket are distinct: Terraform
creates **no** live lake S3 bucket because the app uses MinIO API semantics.
The S3 bucket is for operator-approved backup archives only. Default Compose
(`docker compose up -d --build --wait`) remains the development path and is
not altered by this release overlay.
