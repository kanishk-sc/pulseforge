# Phase 5: observability and reliability design

## Boundaries

Spark remains the only owner of continuous ingestion and its MinIO checkpoints. Airflow
continues to run finite dbt and detector work. A successful product publication remains
the only analytics generation served by the product API. Operational alerts do not
create business incidents. No telemetry call belongs in a warehouse or publication
transaction, and telemetry outages must leave those transactions and API responses
unchanged.

The existing `/health` is process liveness, `/ready` checks PostgreSQL, Kafka and S3,
`/api/v1/analytics/status` describes the last successful product publication, and
`/api/v1/pipeline/status` deliberately reports analytics only. None is an end-to-end
availability signal. Redis is an optional cache and is not part of readiness.

## Signal contract

All timestamps below are Unix seconds in UTC. Counters in process memory reset on
restart; use Prometheus `rate`/`increase` over the scrape series, not a sum across
unrelated process lifetimes. Gauges derived from PostgreSQL are recomputed on each
scrape and carry no build, event or request identifier labels. All labels are fixed
enumerations or framework route templates. A missing series means **unknown**, never
zero or healthy. Scrape `up=0` means only that Prometheus cannot collect the target.

| Signal | Source and meaning | Type / units | Bounded labels | Reset, freshness and authority |
| --- | --- | --- | --- | --- |
| `pulseforge_http_requests_total` | Completed FastAPI requests | counter / requests | standard method or `OTHER`, route template, `2xx`/`3xx`/`4xx`/`5xx` | Resets with API; diagnostic traffic, excludes `/metrics` |
| `pulseforge_http_request_duration_seconds` | Wall time of API handling | histogram / seconds | standard method or `OTHER`, route template | Resets with API; diagnostic, not end-to-end latency |
| `pulseforge_dependency_failures_total` | Failed readiness, warehouse or cache operation | counter / operations | `postgres`/`kafka`/`s3`/`redis`, `error`/`timeout` | Resets with API; diagnostic; absence is not proof of availability |
| `pulseforge_cache_operations_total` | Redis read/write outcome including fallback | counter / operations | `get`/`set`, `hit`/`miss`/`stored`/`error`/`timeout` | Resets with API; diagnostic; misses and bypasses do not change business data |
| `pulseforge_producer_deliveries_total` | Kafka acknowledged sends or failed send attempts | counter / attempts | `confirmed`/`failed` | Resets with producer; process must be running to scrape; confirmed sends are not warehouse inserts |
| `pulseforge_producer_running` | Producer process started and accepting a loop | gauge / boolean | none | Disappears when process exits; no scrape means unknown, not idle |
| `pulseforge_stream_last_progress_timestamp_seconds` | Spark listener's latest successful progress event | gauge / epoch seconds | `raw`/`dlq`/`valid-events`/`validation-counts` | Resets/disappears on driver restart; diagnostic progress, not consumer-group lag |
| `pulseforge_stream_input_rows_total` | Spark reported input rows of completed batch attempts | counter / rows | query name | Resets on driver restart; **attempts** can repeat after checkpoint recovery |
| `pulseforge_stream_processed_rows_per_second` | Spark's reported processing rate | gauge / rows/s | query name | Last batch only; approximate and absent before first progress |
| `pulseforge_stream_batch_duration_seconds` | Spark trigger duration | histogram / seconds | query name | Resets on driver restart; diagnostic |
| `pulseforge_stream_watermark_drops_total` | Spark state operator watermark drops | counter / rows | query name | Resets on driver restart; reported completed batch attempts, may repeat on replay |
| `pulseforge_stream_source_offset` | Spark raw query's last reported ending offset | gauge / offset | fixed partition 0–2 | Not Kafka consumer-group committed lag; offset can move backward after checkpoint changes |
| `pulseforge_stream_validation_rejection_attempts_total` | Diagnostic validation query's grouped invalid rows | counter / attempts | fixed contract error reason | Resets on driver restart; retry/replay may count the same raw source again; DLQ is separate |
| `pulseforge_stream_sink_input_attempt_rows_total`, `pulseforge_stream_sink_inserted_attempt_rows_total` | Successful sink callback's presented/inserted rows | counters / attempt rows | none | Reset on driver restart; replay may repeat callback outcomes; durable ledger is authoritative |
| `pulseforge_stream_sink_failures_total` | Failed `foreachBatch` sink attempts | counter / attempts | `database`/`lake`/`other` | Resets on driver restart; diagnostic |
| `pulseforge_warehouse_committed_rows` | Unique event rows in PostgreSQL | gauge / rows | none | Recomputed per scrape; authoritative committed state, includes replay-safe uniqueness |
| `pulseforge_warehouse_source_max_ingested_timestamp_seconds` | Latest warehouse source ingestion time | gauge / epoch seconds | none | Absent until a row exists; authoritative source activity, not event-time delay |
| `pulseforge_analytics_builds` | Durable product build records by status | gauge / builds | `running`/`succeeded`/`failed` | Recomputed per scrape; authoritative attempt state |
| `pulseforge_analytics_latest_attempt_failed`, `pulseforge_analytics_consecutive_failures` | Newest build state and consecutive failures, latter capped at 100 | gauges / boolean, attempts | none | Recomputed per scrape; missing latest means no attempts, not healthy |
| `pulseforge_analytics_last_duration_seconds` | Newest finished build duration | gauge / seconds | none | Missing until a completed build; diagnostic duration, not current build progress |
| `pulseforge_analytics_last_success_timestamp_seconds` | Latest successful immutable publication | gauge / epoch seconds | none | Absent until published; authoritative build completion; age alone is not a freshness failure during idle traffic |
| `pulseforge_analytics_source_max_ingested_timestamp_seconds` | Frozen staging source watermark for last success | gauge / epoch seconds | none | Absent until a nonempty successful build; authoritative published coverage |
| `pulseforge_analytics_quality_results` | dbt result statuses in latest successful artifact | gauge / results | `pass`/`success` | Recomputed from persisted publication metadata; authoritative for that artifact only |
| `pulseforge_analytics_publication_present`, `pulseforge_analytics_source_event_count` | Whether a success exists and frozen source size | gauges / boolean, events | none | Recomputed per scrape; publication presence alone does not establish freshness |
| `pulseforge_detector_evaluations` | Durable finite detector invocations | gauge / runs | `succeeded`/`skipped`/`failed`, bounded skip reason | Recomputed per scrape; attempts, including retries, remain visible |
| `pulseforge_detector_last_evaluation_timestamp_seconds` | Last finished detector invocation | gauge / epoch seconds | none | Absent until a run; recency is separate from success |
| `pulseforge_detector_incidents_created` | Count of durable rows in `product.incidents` | gauge / incidents | none | Recomputed per scrape; includes earlier evaluations and remains correct if best-effort run telemetry fails |
| `pulseforge_detector_last_duration_seconds` | Last finished evaluation duration | gauge / seconds | none | Missing until a completed run; latest outcome must be checked separately |

The finite job exporter must fail the scrape when PostgreSQL is unavailable. It never
replays a stale cached success. A 15-second scrape of a one-off producer may miss the
entire process; the producer JSON completion log is the authoritative finite-run record.
Spark exposes offsets from its raw query's `endOffset`; it does not establish a Kafka
consumer group or expose an independently sampled Kafka log end offset. The dashboard
therefore says **processed offset**, never committed consumer lag. Warehouse row counts
are read from PostgreSQL, where deduplication and checkpoint retries have resolved.
The validation-count query has its own checkpoint but is optional: failure to start or
later termination loses only that diagnostic. The raw, DLQ and valid-event queries
remain the supervisor's critical set and continue to own business progress.

## Trace contract

FastAPI server spans and child PostgreSQL/Redis operation spans form a request-local
trace. Sampling is parent-based with a bounded root probability; a bounded queue and
short OTLP export timeout prevent collector stalls from delaying responses. SQL text,
bound parameters, Redis keys and values, request bodies, and authorization headers are
not exported. Structured request logs include the existing request ID plus trace and
span IDs when a valid context exists; unsampled IDs will not resolve in Tempo. This
does not constitute a trace across Kafka,
Spark, dbt and the browser; source event IDs and build publication metadata provide
that lineage.

The optional local collector accepts OTLP/HTTP and exports to local Tempo. Grafana
provisions Tempo and Prometheus data sources. Prometheus and Tempo use short local
retention. No telemetry service is a dependency of `/health`, `/ready`, ingestion or
the product API.

## Alerts and interpretation

Alerts use infrastructure labels only. API error rate requires at least 20 requests
in five minutes and a rate above 5% for two minutes. Streaming staleness is evaluated
only while producer confirmed-delivery rate shows active traffic; an idle synthetic
system is not a stalled stream. A failed latest analytics attempt and repeated failed
attempts are separate from publication age. Publication lag requires both a successful
publication over one hour old and committed warehouse source newer than that build's
frozen source watermark. An idle warehouse alone never triggers it. Scrape
unavailability is its own alert. Alert rules have deterministic `promtool test rules`
fixtures for firing, suppression and missing-data cases. No external routing is
configured.

## Failure and load acceptance

Drills use the local disposable Compose stack, record before/failure/recovery evidence,
and restore stopped services. They never remove volumes or checkpoints. The bounded
load harness records actual requests, errors, latency percentiles, throughput, cache
state and resource observations in JSON; old synthetic event timestamps are not
processing latency. Results are local measurements, never a production SLA.
