# PulseForge local demo (2–3 minutes)

This is a script for a **synthetic**, local Compose demonstration, not a hosted
service or an AWS deployment. The [overview](assets/overview-stale.png),
[incident evidence](assets/incident-evidence.png), and
[offline explanation](assets/offline-explanation.png) are genuine browser
captures from 2026-09-28. They show preserved *historical* data; the stale
banner is intentional and must not be presented as live freshness. The
browser recorder could not start without `ffmpeg`, so no video is claimed.

## Prepare on a fresh development project

Use a clean clone or a development project whose existing data you intend
to keep. These commands add synthetic events; do **not** run them against a
shared or production database. Allow roughly 8 GB to Docker for Spark plus
Airflow. In PowerShell from the repository root:

```powershell
uv sync --frozen
uv run python scripts/init_env.py
docker compose up -d --build --wait --wait-timeout 180
docker compose --profile streaming up -d --build --wait --wait-timeout 240 streaming
docker compose --profile product run --rm --build product-migrate
uv run python -m pulseforge.assistant.cli migrate
```

`init_env.py` preserves an existing ignored `.env` and never prints its
generated credentials. The services bind host ports to loopback only. The
assistant provider must remain disabled; the offline explanation does not
need a paid API. A fresh installation without a local runbook index may label
retrieval `lexical_fallback` or `unavailable`; neither should be described
as semantic model retrieval. The optional pinned local-model setup is in the
[README](../../README.md#explain-an-existing-incident-phase-6-optional-setup).

Send a *bounded* real Kafka sample and verify that the stream committed it:

```powershell
docker compose run --rm -e ANOMALY_RATE=0 producer python -m pulseforge.producer --count 50
docker compose exec postgres psql -U pulseforge -d pulseforge -c "SELECT count(*) FROM stream_events;"
uv run python -m pulseforge.streaming.inspect --layer raw --limit 3
```

Wait for the SQL count to include the 50 new producer events before building.
The inspection command shows committed raw lake objects, not a scan of
in-progress files. The username/database shown are the generated setup
defaults; substitute your `.env` values if changed. Existing rows make the
total exceed 50, so do not claim the total is solely from this run.

Seed a separate deterministic payment anomaly through the replay-safe
**warehouse sink**. Those 140 fixture rows are not Kafka/lake evidence:

```powershell
$seed = uv run python scripts/seed_product_acceptance.py | ConvertFrom-Json
$seed | Select-Object events_requested,events_inserted,detector_now,region
docker compose --profile airflow build airflow
$buildKey = "portfolio-demo-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"
docker compose --profile airflow run --rm --no-deps airflow python -m pulseforge.product.cli pipeline --build-key $buildKey --project-dir /opt/pulseforge/analytics --profiles-dir /opt/pulseforge/analytics --detector-now $seed.detector_now
docker compose --profile product up -d --build --wait --wait-timeout 180 redis api dashboard
```

The finite pipeline runs dbt/tests, publishes only a successful analytics
build, then evaluates detectors. Expect one critical payment-failure incident
for `ap-south` with 20 source-evidence rows when the fixture owns its
evaluation hour. If reusing a project, the seed is idempotent within that
hour and an existing incident may be reused; inspect the actual output.
For Bash, use the equivalent environment/JSON handling and a unique
`--build-key`; the [README](../../README.md#run-the-product-and-observability-layers)
contains the shell-neutral service commands.

## Narration and navigation

| Time | Show | What the screen proves—and does not prove |
| --- | --- | --- |
| 0:00–0:25 | `http://127.0.0.1:5173` → **Overview**, then **Analytics status** | Build identity, source count, freshness and quality state are visible. An empty or stale banner is not a payment anomaly; `/health` alone says nothing about data freshness. |
| 0:25–0:55 | **Payment health**, region `ap-south`, 24-hour window | A newly seeded fixture uses the last complete UTC hour, so the published dbt mart supplies a visible denominator and failure rate. Older screenshots may have no points in this window; do not present them as current metrics. |
| 0:55–1:25 | **Incidents** → the critical payment finding | Deterministic threshold, baseline, original build and 20 persisted event references support the finding. The fixture events came from the warehouse sink; the earlier producer sample separately proves Kafka/Spark/lake delivery. |
| 1:25–2:00 | **Explain this incident** | The default offline response separates observed facts, supported interpretation, hypotheses, steps and limitations. Click a citation to locate its source. It is a read-only evidence summary, not an LLM diagnosis or proof that a runbook applies. |
| 2:00–2:30 | Return to **Analytics status**; optionally run the Redis fallback below | Only a fully tested build is published. The optional drill demonstrates one real failure behavior; the separate [recovery record](../deployment/acceptance.md) documents matched SQL and representative lake/checkpoint hashes, not a restore performed by this screen tour. |

If no incident appears, check the seed's `events_inserted`, pipeline exit
status, detector time, region/window and Analytics status before narrating a
result. Do not edit incident or metric tables to make the demo look good.
If the warehouse is unavailable, the UI shows an explicit error; if Redis
fails, API reads bypass the cache. The [verification log](../verification.md)
records actual failure-path checks. A stale build remains visible but
detectors do not classify it as a new business anomaly.

On a **project you own**, a brief optional Redis failure drill verifies that
the same published metric remains available from PostgreSQL. Run after the
successful pipeline; keep the warehouse and API running:

```powershell
docker compose stop redis
try {
  $response = Invoke-WebRequest 'http://127.0.0.1:8000/api/v1/payment-health?region=ap-south'
  "HTTP $($response.StatusCode); X-Cache=$($response.Headers['X-Cache'])"
} finally {
  docker compose --profile product up -d --wait redis
}
```

Expected: HTTP 200 and `X-Cache=bypass`; the `finally` block restores
Redis even if the request fails. A different result is a failed drill to
investigate, not evidence to edit away. This shows cache-failure isolation
only; it does not establish throughput or availability under host failure.

## Teardown and evidence limits

```powershell
docker compose stop producer streaming dashboard api redis
```

This stops the demo's compute while retaining PostgreSQL, Kafka, MinIO,
named volumes and Spark checkpoints. If you own the entire fresh project,
`docker compose down` additionally stops/removes containers and network
but preserves volumes; **never use `down -v`** for this demo. The checked-in
screenshots are small PNGs from a real local UI, not generated mockups. No
recording was committed: `agent-browser record start` failed because
`ffmpeg` was not installed; record the narrated route manually if a video
is needed. Do not upload local data or expose these unauthenticated ports.
