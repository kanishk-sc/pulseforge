CREATE SCHEMA IF NOT EXISTS product;

CREATE TABLE IF NOT EXISTS product.schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS product.analytics_builds (
    build_id uuid PRIMARY KEY,
    build_key text NOT NULL UNIQUE,
    dbt_invocation_id text UNIQUE,
    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    published_at timestamptz,
    source_max_event_ts timestamptz,
    source_max_ingested_at timestamptz,
    source_event_count bigint,
    manifest_sha256 text,
    failure_reason text,
    CHECK ((status = 'running') OR completed_at IS NOT NULL),
    CHECK ((status <> 'succeeded') OR published_at IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS analytics_builds_latest_success
    ON product.analytics_builds (published_at DESC) WHERE status = 'succeeded';

CREATE TABLE IF NOT EXISTS product.payment_health_hourly (
    build_id uuid NOT NULL REFERENCES product.analytics_builds(build_id) ON DELETE CASCADE,
    metric_hour_utc timestamptz NOT NULL,
    region_code text NOT NULL,
    payment_attempt_count bigint NOT NULL,
    successful_payment_count bigint NOT NULL,
    failed_payment_count bigint NOT NULL,
    failure_rate numeric(12,6),
    attempted_amount numeric(24,2) NOT NULL,
    successful_payment_amount numeric(24,2) NOT NULL,
    failed_payment_amount numeric(24,2) NOT NULL,
    PRIMARY KEY (build_id, metric_hour_utc, region_code)
);

CREATE TABLE IF NOT EXISTS product.revenue_hourly (
    build_id uuid NOT NULL REFERENCES product.analytics_builds(build_id) ON DELETE CASCADE,
    metric_hour_utc timestamptz NOT NULL,
    region_code text NOT NULL,
    successful_payment_count bigint NOT NULL,
    revenue_amount numeric(24,2) NOT NULL,
    PRIMARY KEY (build_id, metric_hour_utc, region_code)
);

CREATE TABLE IF NOT EXISTS product.shipment_health_hourly (
    build_id uuid NOT NULL REFERENCES product.analytics_builds(build_id) ON DELETE CASCADE,
    metric_hour_utc timestamptz NOT NULL,
    region_code text NOT NULL,
    shipment_created_count bigint NOT NULL,
    delayed_shipment_count bigint NOT NULL,
    shipment_delay_event_count bigint NOT NULL,
    delayed_shipment_rate numeric(12,6),
    PRIMARY KEY (build_id, metric_hour_utc, region_code)
);

CREATE TABLE IF NOT EXISTS product.refund_requests_hourly (
    build_id uuid NOT NULL REFERENCES product.analytics_builds(build_id) ON DELETE CASCADE,
    metric_hour_utc timestamptz NOT NULL,
    region_code text NOT NULL,
    refund_request_count bigint NOT NULL,
    requested_amount numeric(24,2) NOT NULL,
    PRIMARY KEY (build_id, metric_hour_utc, region_code)
);

CREATE TABLE IF NOT EXISTS product.operations_health_hourly (
    build_id uuid NOT NULL REFERENCES product.analytics_builds(build_id) ON DELETE CASCADE,
    metric_hour_utc timestamptz NOT NULL,
    region_code text NOT NULL,
    order_count bigint NOT NULL,
    successful_payment_count bigint NOT NULL,
    revenue_amount numeric(24,2) NOT NULL,
    payment_attempt_count bigint NOT NULL,
    failed_payment_count bigint NOT NULL,
    payment_failure_rate numeric(12,6),
    shipment_created_count bigint NOT NULL,
    delayed_shipment_count bigint NOT NULL,
    shipment_delay_event_count bigint NOT NULL,
    delayed_shipment_rate numeric(12,6),
    refund_request_count bigint NOT NULL,
    refund_requested_amount numeric(24,2) NOT NULL,
    refund_requests_per_order numeric(12,6),
    PRIMARY KEY (build_id, metric_hour_utc, region_code)
);

CREATE TABLE IF NOT EXISTS product.incidents (
    incident_id uuid PRIMARY KEY,
    uniqueness_key text NOT NULL UNIQUE,
    detector_name text NOT NULL,
    detector_version text NOT NULL,
    region_code text NOT NULL,
    evaluation_start timestamptz NOT NULL,
    evaluation_end timestamptz NOT NULL,
    observed_metric numeric(24,6) NOT NULL,
    observed_denominator bigint,
    baseline_metric numeric(24,6) NOT NULL,
    threshold numeric(24,6) NOT NULL,
    severity text NOT NULL CHECK (severity IN ('warning', 'critical')),
    summary text NOT NULL,
    analytics_build_id uuid NOT NULL REFERENCES product.analytics_builds(build_id),
    detected_at timestamptz NOT NULL DEFAULT now(),
    status text NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'resolved')),
    resolved_at timestamptz,
    CHECK (evaluation_start < evaluation_end),
    CHECK ((status = 'open' AND resolved_at IS NULL) OR status = 'resolved')
);
CREATE INDEX IF NOT EXISTS incidents_list
    ON product.incidents (detected_at DESC, incident_id DESC);

CREATE TABLE IF NOT EXISTS product.incident_evidence (
    incident_id uuid NOT NULL REFERENCES product.incidents(incident_id) ON DELETE CASCADE,
    source_event_id uuid NOT NULL,
    evidence_role text NOT NULL,
    event_ts timestamptz NOT NULL,
    PRIMARY KEY (incident_id, source_event_id, evidence_role)
);
