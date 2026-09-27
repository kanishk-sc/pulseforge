# Quiesced backup and separate-project restore

The platform has coupled durable state: PostgreSQL `stream_events`, batch
ledger, analytics publications/incidents and assistant metadata; MinIO
raw/cleaned/curated objects, commit manifests and Spark checkpoints; and
Kafka records/offsets. Airflow state is useful for finite-job history, while
Redis, model vectors/cache and local telemetry are rebuildable or diagnostic.
Copying these volumes at unrelated times is **not** a consistent backup.
The supported local drill stops every container in one explicitly isolated
project, then archives all existing named volumes to checksummed tar files.
The script refuses the default development project, active containers,
missing core volumes, target-volume overwrite and archive checksum mismatch.
It does not delete anything or upload to S3.

```sh
export PULSEFORGE_RELEASE_SHA="$(git rev-parse HEAD)"
docker compose -p pulseforge-p7-source -f docker-compose.yml -f infra/docker/compose.release.yml --profile '*' stop
uv run python scripts/deployment/recovery.py backup --project pulseforge-p7-source --archive .pytest_cache/p7-backup-001
uv run python scripts/deployment/recovery.py restore --project pulseforge-p7-restored --archive .pytest_cache/p7-backup-001
docker compose -p pulseforge-p7-restored -f docker-compose.yml -f infra/docker/compose.release.yml up -d --no-build --wait --wait-timeout 240 postgres kafka minio bootstrap
```

On PowerShell set `$env:PULSEFORGE_RELEASE_SHA=(git rev-parse HEAD)` before
the Compose commands. Use a **new** archive directory and project name each
time. `manifest.json` stores per-volume SHA-256 and byte counts. Tar archives
contain plaintext database passwords/tokens and potentially customer data;
restrict local filesystem access, retention and transfer. Terraform creates
an encrypted/versioned S3 bucket and a prefix-scoped host role, but this tool
does **not** upload anything; an authorized operator must separately encrypt,
transfer, set retention and verify remote copies. Merely copying a tar to S3
does not validate a restore.

After restore, compare against a pre-stop inventory, not only object counts:

- PostgreSQL distinct `(event_id, source_topic, source_partition, source_offset)`
  identity, batch ledger count, warehouse aggregate totals, latest successful
  build ID/status, incident source IDs/evidence and assistant index versions.
- SHA-256 of representative nonempty raw/cleaned/curated Parquet objects, a
  lake commit manifest and a checkpoint commit before/after. Verify Kafka
  topic partitions and retained offsets/records, then restart **one** Spark
  owner and confirm no duplicate business effects. If Kafka retention has
  expired, replay from retained lake/source backup requires a separate
  explicit procedure; this drill does not invent lost broker records.
- If corpus was indexed, run a representative pgvector retrieval against the
  restored assistant schema and model cache, or label lexical fallback when
  model assets are absent. Verify API publication/freshness and browser states
  before promoting the restored project.

The read-only `state_probe.py` comparison in the [local runbook](local-runbook.md)
implements SQL identity/mart/build/evidence/corpus checks and representative
five-class object-content hashes. It is deliberately a sample, **not** a
full-object audit. Record `kafka-get-offsets.sh --time -2` and `--time -1`
around the drill to show retained start/end offsets. After starting the
restored Spark owner, repeat SQL event/source counts and batch-ledger totals;
they must not increase from replay alone. Measurements are in
[acceptance](acceptance.md).

The script archives only *currently present* optional volumes. MinIO or Kafka
can be much larger than a local backup directory; check free space first.
Do not use `down -v` on the source or restore projects. The source volumes are
retained for comparison; clean them only through a separately reviewed
destructive action after backup validation. A successful local restore does
not measure AWS RPO/RTO or survive host/AZ loss. RPO/RTO for a future cloud
deployment are **targets to define and measure**, not Phase 7 results.
