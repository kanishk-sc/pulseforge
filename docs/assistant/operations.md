# Local assistant operations (PowerShell)

This feature runs only on an existing local PulseForge incident. The default
foundation does not need a model. Keep the PostgreSQL named volume and all Spark
checkpoints; never use `docker compose down -v` to set up the assistant. The
`pgvector/pgvector:0.8.6-pg16-bookworm` image retains PostgreSQL major version 16
and the existing `postgres-data` volume. Check the database before/after any image
change with `docker compose exec -T postgres psql -U pulseforge -d pulseforge -Atc
"SELECT current_setting('server_version'), count(*) FROM stream_events GROUP BY 1"`.
The Compose MinIO service builds the same pinned release from a SHA-256-verified
official GitHub asset because its former Quay image became inaccessible to fresh CI
runners. This does not change or replace `minio-data`; first build downloads a
110.99 MB binary. Do not remove the volume to troubleshoot registry availability.

## Optional one-time setup

```powershell
uv sync --frozen
uv run python scripts/init_env.py
docker compose up -d --wait --wait-timeout 180 postgres
uv run python -m pulseforge.assistant.cli migrate
uv run python -m pulseforge.assistant.cli download-model
uv run python -m pulseforge.assistant.cli ingest
uv run python -m pulseforge.assistant.cli retrieve 'payment failure rate incident triage' --top-k 4
docker compose --profile product up -d --build --wait --wait-timeout 180 api dashboard
```

`download-model` fetches exactly revision
`5f1b8cd78bc4fb444dd171e59b18f3a3af89a079` of Qdrant's MiniLM ONNX export
from Hugging Face. The model advertises Apache-2.0, 384 dimensions and about 90 MB.
The measured local cache was 86.9 MiB. A Windows checkout without symlink support
may use more disk. Allow several hundred MiB of spare RAM for CPU inference; no
peak-memory benchmark has been run. All subsequent commands use the local snapshot;
normal API requests never initiate a download. If the local asset is absent, the
assistant returns an explicit `lexical_fallback` mode. If indexing is absent, it
returns `unavailable`. Reindex after any deliberate model/dimension change:

```powershell
uv run python -m pulseforge.assistant.cli ingest --reindex
```

That command only reconciles the `assistant` schema. Deleting a document from the
allowlist retires its chunks on the next ingestion; changed content replaces old
chunks. No remote URL or arbitrary path is accepted. A mismatch without `--reindex`
fails closed.

## Inspect and evaluate

```powershell
$incidents = Invoke-RestMethod 'http://127.0.0.1:8000/api/v1/incidents?status=open'
$incidentId = $incidents.items[0].incident_id
uv run python -m pulseforge.assistant.cli explain $incidentId
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8000/api/v1/incidents/$incidentId/explanation" -ContentType 'application/json' -Body '{"mode":"offline"}'
uv run pytest tests/test_assistant_integration.py --run-integration --run-assistant
uv run python scripts/evaluate_assistant.py --split all --output docs/assistant/eval-results.json
```

Open the dashboard at `http://127.0.0.1:5173`, select the incident and click
**Explain this incident**. Source links point to evidence displayed on the page.
The explanation has no persistent conversation history. Evaluation JSON is a
version-controlled measurement artifact, not user chat retention. Set
`$env:UV_LINK_MODE='copy'` and use `uv run --isolated` if Windows has the development
virtual environment locked by another process.

## Optional provider, not exercised by the Phase 6 acceptance

Live inference transmits the bounded incident/runbook context externally and can
incur charges. It requires separate authorization specifying provider and budget.
After that decision, put `ASSISTANT_PROVIDER=openai`, an explicitly chosen currently
supported `ASSISTANT_PROVIDER_MODEL`, and `ASSISTANT_PROVIDER_KEY` in ignored `.env`.
Do not print `docker compose config` or share its output while the key is set: Compose
may interpolate it. Recreate the API and make one explicit request:

```powershell
docker compose --profile product up -d --no-deps --force-recreate --wait api
$body = '{"mode":"provider"}'
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8000/api/v1/incidents/$incidentId/explanation" -ContentType 'application/json' -Body $body -TimeoutSec 30
```

This command is a smoke-test recipe, **not** evidence that live inference has run.
The frontend never invokes it automatically. A 401/403, timeout, 429, invalid
structure or fabricated citation yields a controlled error; it does not create a
business incident or silently switch providers. Disable the provider after testing
by restoring `ASSISTANT_PROVIDER=disabled`, clearing the key from local configuration,
and recreating the API. Credentials are never version-controlled.

To reclaim optional model disk only, confirm the resolved path is the ignored
project `.cache/pulseforge-model` directory and remove that directory. Leave Docker
volumes, the PostgreSQL data directory and Spark checkpoints untouched. The assistant
will then visibly use lexical fallback until the pinned model is installed again.
