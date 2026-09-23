CREATE TABLE IF NOT EXISTS product.dbt_quality_runs (
    build_id uuid PRIMARY KEY REFERENCES product.analytics_builds(build_id) ON DELETE CASCADE,
    result_count integer NOT NULL CHECK (result_count > 0),
    pass_count integer NOT NULL CHECK (pass_count >= 0),
    success_count integer NOT NULL CHECK (success_count >= 0),
    recorded_at timestamptz NOT NULL DEFAULT now(),
    CHECK (pass_count + success_count = result_count)
);

CREATE TABLE IF NOT EXISTS product.detector_runs (
    run_id uuid PRIMARY KEY,
    analytics_build_id uuid REFERENCES product.analytics_builds(build_id) ON DELETE SET NULL,
    evaluation_at timestamptz NOT NULL,
    started_at timestamptz NOT NULL,
    completed_at timestamptz,
    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'skipped', 'failed')),
    skip_reason text CHECK (skip_reason IN ('no_publication', 'stale_publication')),
    created_incidents integer NOT NULL DEFAULT 0 CHECK (created_incidents >= 0),
    error_type text,
    CHECK ((status = 'running') = (completed_at IS NULL))
);
CREATE INDEX IF NOT EXISTS detector_runs_recent ON product.detector_runs (started_at DESC);
