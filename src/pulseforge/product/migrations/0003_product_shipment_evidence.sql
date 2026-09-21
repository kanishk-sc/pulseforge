ALTER TABLE product.analytics_evidence
    DROP CONSTRAINT IF EXISTS analytics_evidence_evidence_kind_check;

ALTER TABLE product.analytics_evidence
    ADD COLUMN IF NOT EXISTS evaluation_ts timestamptz;

UPDATE product.analytics_evidence
SET evaluation_ts = event_ts
WHERE evaluation_ts IS NULL;

ALTER TABLE product.analytics_evidence
    ALTER COLUMN evaluation_ts SET NOT NULL;

ALTER TABLE product.analytics_evidence
    ADD CONSTRAINT analytics_evidence_evidence_kind_check CHECK (
        evidence_kind IN (
            'payment_attempt',
            'shipment_cohort',
            'shipment_delay',
            'refund_request',
            'order'
        )
    );

DROP INDEX IF EXISTS product.analytics_evidence_lookup;

CREATE INDEX analytics_evidence_lookup
    ON product.analytics_evidence (
        build_id,
        evidence_kind,
        region_code,
        evaluation_ts,
        event_ts
    );
