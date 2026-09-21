CREATE TABLE IF NOT EXISTS product.analytics_evidence (
    build_id uuid NOT NULL REFERENCES product.analytics_builds(build_id) ON DELETE CASCADE,
    source_event_id uuid NOT NULL,
    evidence_kind text NOT NULL CHECK (
        evidence_kind IN ('payment_attempt', 'shipment_cohort', 'refund_request', 'order')
    ),
    event_ts timestamptz NOT NULL,
    region_code text NOT NULL,
    PRIMARY KEY (build_id, source_event_id, evidence_kind)
);

CREATE INDEX IF NOT EXISTS analytics_evidence_lookup
    ON product.analytics_evidence (build_id, evidence_kind, region_code, event_ts);
