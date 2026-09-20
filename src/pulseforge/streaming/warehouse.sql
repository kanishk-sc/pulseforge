CREATE TABLE IF NOT EXISTS stream_events (
    schema_version integer NOT NULL,
    event_id uuid PRIMARY KEY,
    event_type text NOT NULL,
    event_ts timestamptz NOT NULL,
    customer_id text,
    order_id text,
    product_id text,
    amount numeric(14,2) CHECK (amount >= 0),
    currency text NOT NULL,
    payment_provider text,
    shipment_provider text,
    region text NOT NULL,
    status text NOT NULL,
    metadata jsonb NOT NULL,
    source_topic text NOT NULL,
    source_partition integer NOT NULL,
    source_offset bigint NOT NULL,
    source_timestamp timestamptz,
    ingested_at timestamptz NOT NULL,
    UNIQUE (source_topic,source_partition,source_offset)
);
CREATE INDEX IF NOT EXISTS stream_events_time_type ON stream_events(event_ts,event_type);
CREATE INDEX IF NOT EXISTS stream_events_order ON stream_events(order_id);
CREATE TABLE IF NOT EXISTS streaming_batches (
    query_id text NOT NULL,
    batch_id bigint NOT NULL,
    row_count bigint NOT NULL,
    committed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (query_id,batch_id)
);
CREATE TABLE IF NOT EXISTS stream_metrics_minute (
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    event_type text NOT NULL,
    event_count bigint NOT NULL,
    total_amount numeric(24,2) NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start,event_type)
);
-- JDBC has no portable UUID/JSONB writer; cast these two text columns during commit.
CREATE TABLE IF NOT EXISTS stream_event_stage (
    schema_version integer, event_id text, event_type text, event_ts timestamptz,
    customer_id text, order_id text, product_id text, amount numeric(14,2), currency text,
    payment_provider text, shipment_provider text, region text, status text, metadata text,
    source_topic text, source_partition integer, source_offset bigint,
    source_timestamp timestamptz, ingested_at timestamptz
);
