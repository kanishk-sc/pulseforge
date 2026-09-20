# Phase 4 product design

## Product boundary

Phase 4 turns tested dbt marts into a build-aware operations product. It does not
query dbt's mutable relations directly at request time. A successful dbt build is
copied into immutable, build-tagged `product` tables, and every API response identifies
the exact successful publication it serves. Synthetic data is labeled in the UI.

The implemented path is:

```text
stream_events -> dbt build/tests -> repeatable-read publication -> detectors
                                                       |              |
                                                       v              v
                                              versioned metrics    incidents/evidence
                                                       \              /
                                                        FastAPI -> React
```

Redis is an optional response cache. PostgreSQL remains authoritative for builds,
metrics, incidents and evidence.

## Atomic publication and freshness

`product.analytics_builds` is the publication ledger. A build moves through
`running`, `succeeded`, or `failed`. The orchestration command performs these steps:

1. create or resume a `running` build key;
2. run `dbt build`, including all data tests;
3. reject missing, malformed, or non-successful `run_results.json` artifacts;
4. begin one PostgreSQL `REPEATABLE READ` transaction;
5. record source event/ingestion watermarks and count;
6. copy all five current dbt marts into build-tagged product tables; and
7. mark the build successful and publish it in the same transaction.

A running build never replaces the previous success. A dbt or publication failure is
recorded and leaves the prior success serviceable. A publication transaction sees one
consistent source/mart state even while ingestion continues; later events belong to a
subsequent build. The API selects only the newest `succeeded` build. It reports the
newest running/failed metadata separately.

If detection fails after publication, an orchestration retry recognizes the successful
build key, skips dbt/publication, and reruns only idempotent detection. This preserves
the success while making the finite post-publication step recoverable.

Freshness is the age of the successful publication, not `max(event_ts)`. Event time can
legitimately be old or sparse and cannot prove pipeline liveness. The default stale
threshold is two hours. Stale successful data remains visible and explicitly labeled;
detectors do not classify it. With no success, metrics return 503
`analytics_publication_unavailable` rather than fabricated zeros.

## Product schema

The versioned migration creates:

- `product.schema_migrations`;
- `product.analytics_builds`;
- build-tagged payment, revenue, shipment, refund and operations hourly snapshots;
- build-tagged source-event evidence projections for all detector inputs;
- `product.incidents`; and
- `product.incident_evidence`.

Incident uniqueness includes detector name/version, region, evaluation interval and
analytics build. Incidents have an explicit open/resolved lifecycle. Evidence stores
source event UUID, event timestamp and its role in the finding. It does not copy customer
or payment data into the incident row.

## Versioned API contract

The product surface is under `/api/v1`:

| Endpoint | Meaning |
| --- | --- |
| `/payment-health` | Attempts, failures, failure rate and decimal amounts |
| `/revenue` | Successful-payment count and revenue |
| `/shipment-health` | Creation-cohort delay metrics and 24-hour maturity flag |
| `/refund-requests` | Request count and requested amount |
| `/operations-health` | Combined tested operational features |
| `/analytics/status` | Successful build, freshness and running/failed metadata |
| `/pipeline/status` | Publication state; Spark liveness explicitly not observed |
| `/incidents` | Filtered, cursor-paginated incident summaries |
| `/incidents/{id}` | Threshold, build identity and source-event evidence |

Metric windows are timezone-aware UTC `[start,end)` intervals, default to 24 hours and
are limited to 31 days and 2,000 points. Regions are a closed contract enum. Incident
pages allow at most 100 rows. Decimal database values serialize as strings so JavaScript
does not silently round money or detector thresholds.

Cache keys include endpoint, successful build ID, exact interval, region and limit.
TTL is bounded. Redis timeouts/errors bypass caching and read PostgreSQL; errors and 503
responses are never cached. Database failures return a controlled 503. Local Compose
binds ports to loopback and remains unauthenticated; TLS, authentication, authorization,
rate limiting and least-privilege database roles are required before network exposure.

## Deterministic detectors

Version `1.0.0` evaluates the last complete UTC hour from the latest fresh publication.
The current interval is excluded from every baseline.

| Detector | Minimum evidence | Trigger |
| --- | --- | --- |
| Payment failure-rate increase | 20 attempts, 6 historical windows | current >= max(baseline + 0.10, baseline × 1.5) |
| Shipment delay-rate increase | 10 creations, 6 historical mature cohorts | current >= max(baseline + 0.15, baseline × 1.5) |
| Refund-request spike | 20 orders, 6 historical windows | current ratio >= max(baseline + 0.05, baseline × 2) |
| Order-volume drop | 12 historical windows, baseline mean >= 20 | current count <= baseline × 0.5 |

Shipment detection evaluates a creation cohort only after 24 hours. A six-hour cooldown
suppresses repeated open findings for the same detector and region. Stable uniqueness
also makes retries idempotent. Incident and up to 25 ordered evidence references are
inserted in one transaction. Detection is finite and statistical; no LLM participates.

## Dashboard behavior

The React/TypeScript dashboard has Overview, Payment health, Revenue, Shipments,
Refund requests, Incidents and Analytics status views. It renders API data only—there
are no fallback demo numbers. It exposes UTC and interval semantics, build identity,
fresh/stale state, mature shipment cohorts, failed build history and the publication
boundary. Missing denominators render as unavailable rather than zero.

Requests are aborted on view/filter teardown. Successful polling is every 30 seconds;
errors back off exponentially to 120 seconds. Loading, empty, stale, unavailable and
error states are distinct. Incident selection from Overview navigates directly to its
threshold and source evidence. Navigation, filters, controls and focus styles are
keyboard-accessible and responsive.

## Deterministic acceptance fixture

`scripts/seed_product_acceptance.py` commits 140 contract-valid payment events through
the existing replay-safe warehouse ingestion boundary: six baseline hours with 20
successful attempts each and one evaluation hour with 20 failures, all in `ap-south`.
UUID5 event IDs plus a time-scoped source topic and offset set make a same-hour replay
idempotent. It prints the fixed detector clock used by the acceptance pipeline.

The observed end-to-end result is one critical `payment_failure_rate_increase` incident:
observed `1.000000`, denominator `20`, baseline `0.000000`, threshold `0.100000`, with
20 source-event evidence records. A repeated detector evaluation creates zero incidents.

## Local operation

PowerShell and Bash-compatible commands, except where shell variable capture is shown:

```sh
docker compose up -d --wait --wait-timeout 180 postgres
docker compose --profile product run --rm --build product-migrate
uv run python scripts/seed_product_acceptance.py
docker compose --profile airflow build airflow
docker compose --profile airflow run --rm --no-deps airflow python -m pulseforge.product.cli pipeline --build-key local-product-build --project-dir /opt/pulseforge/analytics --profiles-dir /opt/pulseforge/analytics
docker compose --profile product up -d --build --wait --wait-timeout 180 redis api dashboard
```

For a fixed-clock acceptance run, pass the seed script's `detector_now` value as
`--detector-now`. Airflow omits that test-only override and uses the real UTC clock.

## Known limits

- Publication copies full small marts; measured scale may justify partition exchange or
  incremental snapshots, but must preserve atomic build identity.
- Detector baselines are deterministic rules, not seasonal models or causal diagnosis.
- The API reports publication state and deliberately does not infer Spark health.
- Incident resolution is represented in storage but no mutating resolution API is
  exposed in this unauthenticated local phase.
- The acceptance seeder exercises the transactional ingestion boundary directly; Phase
  2 separately verifies Kafka/Spark delivery, watermark, restart and outage recovery.
- No performance, availability, business-impact or model-accuracy claim is made.
