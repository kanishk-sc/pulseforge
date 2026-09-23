# Phase 5 bounded local measurements — 2026-09-23

These are functional measurements of a local development stack, not capacity limits,
availability estimates or production SLAs. The JSON artifacts in this directory are
the raw records, including configuration, Git SHA, Docker stats before/after, status
counts, cache headers and error counts. The Windows 11 host exposed 16 logical CPUs;
Docker Desktop reported 16 CPUs, 12,155,170,816 bytes of memory and Docker 29.5.2.
The harness used Python 3.12.13 and a populated PostgreSQL warehouse (693–793 rows
at the start of these runs). The worktree-clean flag is false because measurement JSON
was being written but not yet committed; source changes were committed at each recorded
SHA. Warm, cold and ingestion used `f0efa47b3dea38f6bd3b99cfff6d486dafe57ea7`;
the final fallback used `a9e2fa6a9639b5761333ce5171fe625efe40f0f7`, whose only
intervening source change made failed acceptance exit nonzero.

The API scenarios targeted `/api/v1/revenue` with a 24-hour UTC window. Warm used
one untimed cache warm-up and a fixed `limit=1000` key. Cold used unique bounded
`limit=1..200` keys. Redis was stopped only for fallback and restored in a `finally`
block. The request scheduler targeted 20/s for warm/cold and 5/s for fallback;
concurrency was capped at four. Percentiles are client-observed HTTP wall time for
responses received, not server histogram values or browser render time.

| Scenario | Requests / result | Duration | Throughput | p50 | p95 | p99 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Warm cache | 200/200 HTTP 200, 200 hits, 0 errors | 9.975 s | 20.051 req/s | 12.258 ms | 14.375 ms | 18.045 ms |
| Cold cache | 200/200 HTTP 200, 200 misses, 0 errors | 9.956 s | 20.089 req/s | 14.840 ms | 21.469 ms | 49.423 ms |
| Redis fallback, successful repeat | 50/50 HTTP 200, 50 bypasses, 0 errors | 10.321 s | 4.844 req/s | 516.426 ms | 521.098 ms | 665.061 ms |

Ingestion sent 100 clean, unique synthetic events at a configured 10/s with anomaly
injection explicitly disabled. The producer took 20.812 s wall time; PostgreSQL
committed row count grew from 693 to 793. The harness observed all 100 rows 13.078 s
after producer exit, with two-second polling. This is a **drain observation**, not
per-event processing latency; event-time delay was not measured. Contract rejection,
duplicates and checkpoint recovery are covered by separate integration tests. An
earlier pre-fix run left anomaly injection at its default 2%, produced only 98 new
warehouse rows from 100 sends, and correctly did not satisfy the clean-row assertion.

One fallback repeat was interrupted: 44/50 HTTP 200 and six client transport errors
over 177.299 s. It is retained as
[`phase5-fallback-interrupted.json`](phase5-fallback-interrupted.json), not averaged
into the successful result. The API logged many normal ~0.5 s bypass responses around
large wall-clock gaps and Redis name-resolution errors while Redis was stopped; the
exact cause of the client interruption was not established. The subsequent repeat
passed 50/50. The harness now exits nonzero when HTTP errors, wrong cache state or
incomplete ingestion make a scenario fail, while still preserving the JSON evidence.

Reproduction, from the repository root after starting the documented product,
streaming and observability profiles with populated PostgreSQL:

```powershell
$env:UV_LINK_MODE='copy'
uv run --isolated python scripts/load_phase5.py --scenario warm --requests 200 --concurrency 4 --request-rate 20 --output docs/benchmarks/phase5-warm.json
uv run --isolated python scripts/load_phase5.py --scenario cold --requests 200 --concurrency 4 --request-rate 20 --output docs/benchmarks/phase5-cold.json
try {
  docker compose stop redis
  uv run --isolated python scripts/load_phase5.py --scenario fallback --requests 50 --concurrency 4 --request-rate 5 --output docs/benchmarks/phase5-fallback.json
} finally { docker compose --profile product up -d --wait redis }
uv run --isolated python scripts/load_phase5.py --scenario ingestion --events 100 --rate 10 --output docs/benchmarks/phase5-ingestion.json
```

These short local observations do not determine saturation throughput, tail latency
under sustained concurrency, multi-node reliability or a safe production alert SLO.
