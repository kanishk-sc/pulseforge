# Phase 7 deployment acceptance

This report separates locally executed image/Compose/recovery evidence from
credential-free Terraform validation and unexecuted AWS operation. Record
the exact tested commit, commands, results and failures before treating a
release as reviewable. No AWS plan/apply/destroy, registry push, public
exposure, data upload or billable model inference is authorized in Phase 7.

## Executed isolated local path (2026-09-27)

Windows 11/PowerShell host, Docker Desktop Linux x86_64 Engine 29.5.2 and
Compose v5.1.4, approximately 12.2 GB allocated Docker memory. App images
were built from clean source commit
`8a3d8f52dfc25bd442acc29d12dde93fe8740aea`; no image was pushed.
The `localhost` dashboard-health failure was found during this drill and
corrected in the release override to `127.0.0.1`. The read-only state probe
was also added after the initial image build. Final PR-head image/CI checks
are separate gates recorded in the PR; this drill does not silently claim
that a later commit was executed end to end.

| Exact command or bounded operation | Observed result |
| --- | --- |
| `$env:UV_LINK_MODE='copy'; uv run --isolated python scripts/deployment/release.py build --project pulseforge-p7-accept` | Six Linux/amd64 commit-tagged images built; local manifest recorded image IDs, SHA and `published_externally=false`. A preceding ordinary `uv run` later hit a OneDrive `.venv` access error; `--isolated` with copy-link mode worked. |
| `docker compose -p pulseforge-p7-accept -f docker-compose.yml -f infra/docker/compose.release.yml up -d --no-build --wait --wait-timeout 240 postgres kafka minio bootstrap` | Fresh isolated PG/Kafka/MinIO volumes, private bridge and loopback host ports; all three data services healthy and bootstrap completed. Existing development project was not stopped or reset. |
| Same Compose project `run --rm product-migrate` twice; `run --rm assistant-migrate` twice | Product versions `0001`–`0004` then `none`; assistant `0001` then `[]`. `pytest -q tests/test_deployment_integration.py --run-integration --run-deployment` against the isolated PG port passed 1/1, including competing migration and pipeline-writer locks in a disposable database. |
| `--profile streaming up -d --no-build --wait --wait-timeout 240 streaming`; `run --rm -e ANOMALY_RATE=0 producer python -m pulseforge.producer --count 50` | One Spark owner healthy; producer confirmed 50 deliveries; PostgreSQL `stream_events` reached 50. |
| `--profile airflow run --rm --no-deps airflow python -m pulseforge.product.cli pipeline --build-key p7-accept-8a3d8f5 --project-dir /opt/pulseforge/analytics --profiles-dir /opt/pulseforge/analytics` | Populated dbt build PASS=109, WARN=0, ERROR=0, SKIP=0; successful immutable publication `06add965-3f03-4e8d-9ab6-ed67cfbc6d77`, zero incidents. |
| `run --rm assistant-model`; `run --rm assistant-ingest` | Explicit public model download reported pinned 384-dimensional local model; 3 versioned documents and 24 embedded chunks indexed. No provider inference. |
| `$env:POSTGRES_HOST='127.0.0.1'; $env:POSTGRES_PORT='15432'; ...; uv run --isolated python scripts/seed_product_acceptance.py`; second finite pipeline with build key `p7-accept-incident-8a3d8f5` and printed detector clock | The established fixture inserted 140 synthetic replay-safe **warehouse** rows (not Kafka/lake rows); dbt PASS=109 and published `b05de9eb-c18d-4fa7-95d4-7d77a0b9c2d0`, one critical payment-failure incident and 20 evidence records. Total warehouse rows 190. |
| `--profile product up -d --no-build --wait --wait-timeout 180 redis api dashboard` | First attempt failed only the newly added dashboard probe (`wget http://localhost/` resolved to an unavailable address). After changing it to `127.0.0.1`, the same bounded readiness command passed; `/ready` returned PG/Kafka/object storage `up`. |
| `npx agent-browser --session p7-accept open http://127.0.0.1:15173`; snapshot; click critical incident; click **Explain this incident** | Real Chromium flow showed the one critical incident, its original successful build, observed 1.000/denominator 20 vs baseline 0.000/threshold 0.100, 12/20 displayed source references and semantic offline retrieval with runbook citations. Browser closed. |
| Stop isolated Redis, call bounded payment-health API, restart Redis | The same eight points returned HTTP 200 with `X-Cache: bypass`; Redis returned healthy. This checked failure isolation, not load capacity. |
| `uv run --isolated python scripts/deployment/state_probe.py --project pulseforge-p7-accept --output .pytest_cache/p7-pre-restore.json`; repeat with `--compare` before stop | Probe returned 190 events with both event-ID and source-position uniqueness, 4 ledger batches, published build, one incident/20 evidence rows, 24 corpus chunks and five nonempty object classes. Repeat matched exactly. |
| `docker compose -p pulseforge-p7-accept -f docker-compose.yml -f infra/docker/compose.release.yml --profile '*' stop`; `uv run --isolated python scripts/deployment/recovery.py backup --project pulseforge-p7-accept --archive .pytest_cache/p7-backup-001`; `restore --project pulseforge-p7-restored --archive .pytest_cache/p7-backup-001` | All source containers stopped; six existing named volumes archived with SHA-256 and restored into new labeled volumes. No default-development or source volume was overwritten/deleted. |
| Start restored PG/Kafka/MinIO/bootstrap with `--wait --wait-timeout 240`; `state_probe.py --project pulseforge-p7-restored --compare .pytest_cache/p7-pre-restore.json` | Exact snapshot matched, including SQL identities, mart totals, build/evidence/corpus state and **representative content SHA-256** from raw, cleaned, curated, commit and checkpoint prefixes; not a full-object audit. |
| Start restored single streaming owner; query `count(*), count(distinct event_id), count(distinct (source_topic,source_partition,source_offset))` and batch ledger | Spark healthy; counts remained `190|190|190`, ledger `4|190` after restart. Kafka retained start offsets 0/0/0 and end offsets 17/16/17 (50 produced messages). No duplicate business effect observed. |
| Stop restored data services; start only source PG/MinIO, run updated `state_probe.py --project pulseforge-p7-accept --output .pytest_cache/p7-source-parquet.json`; stop source, start only restored PG/MinIO, run `state_probe.py --project pulseforge-p7-restored --compare .pytest_cache/p7-source-parquet.json` | Exact match again **after** restored stream restart and intentional failed build. The tightened probe selected an actual nonempty `.parquet` object from each raw/cleaned/curated prefix, a `.json` commit manifest and a checkpoint `commits/0` object; it no longer accidentally sampled raw `_spark_metadata`. Both projects were then stopped without volume deletion. |
| Restored `api python -m pulseforge.assistant.cli retrieve 'payment failure rate incident triage'`; restored browser incident/explanation flow | Retrieval mode `semantic` with ranked versioned sections. Restored dashboard and API healthy; same incident/build and offline explanation visible in Chromium. Browser closed. |
| `--profile airflow run --rm --no-deps airflow python /opt/pulseforge/scripts/verify_airflow_dag.py`; intentionally invalid `pipeline --build-key p7-intentional-failure --project-dir /no-such-project ...` | DAG imports zero errors and only `verify_warehouse`, `dbt_build`, `quality_summary` finite tasks. Invalid dbt step exited 1; `p7-intentional-failure` recorded `failed`, while previous `p7-accept-incident-8a3d8f5` remained `succeeded`. |
| `docker run --rm --entrypoint sha256sum pulseforge-minio:8a3d8f5… /usr/local/bin/minio`; `docker compose config --quiet` | MinIO binary SHA-256 was `7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f`, the Dockerfile's expected asset hash; default development Compose still parsed. This is an availability repair, not a security upgrade. |

The restored project was stopped without `-v`; source/restored named volumes
remain available for review. The local backup is an ignored, sensitive test
artifact. RPO and RTO were **not** measured and are not promised. The
intentional failing pipeline altered only the disposable restored project.

## Static checks and remaining gates

Terraform 1.10.5 `fmt -check -recursive`, `init -backend=false` and
`validate -no-color` passed using the pinned provider without credentials.
After adding the checksum-pinned Compose host prerequisite,
`docker run --rm --mount "type=bind,source=$((Get-Location).Path),target=/workspace,readonly" python:3.12-slim-bookworm bash -n /workspace/infra/terraform/user-data.sh`
passed shell syntax; it did **not** execute user data on Amazon Linux.
No mocked-provider test, account-aware plan, AWS apply, AMI/SSM test, EBS
mount/reboot test or cost measurement was performed. The Linux deployment
and recovery CI jobs must still run on the exact final PR head. The local
hosted-CI-independent checks include Ruff format/lint, `uv lock --check`,
exported schema, 123 non-integration/non-Spark Python tests, 5 release-tool
unit tests, 1 isolated migration/concurrency integration test, 8 frontend
tests, TypeScript typecheck/build, and npm audit (zero reported findings).
See [verification](../verification.md) and the PR's checks for exact final
head results. None of these claims establishes live AWS readiness or a
production security posture.

The attempted exact hosted selection
`uv run --isolated pytest -q -m 'not integration' --basetemp .pytest_cache/p7-full-final`
was interrupted after its non-Spark cases when a local Spark transform case
stalled under the Windows host's Java 8 (`java -version` = 1.8.0_481).
Spark 4 requires a suitable newer JVM; this interrupted run is **not** a
pass. The 123-case non-Spark selection passed separately. Actual Spark
streaming was exercised in the Java-17 release container; the existing
hosted Linux Python/streaming jobs must establish the full regression gate.
[Spark 4.0.1 documents Java 17/21 support](https://spark.apache.org/docs/4.0.1/).
