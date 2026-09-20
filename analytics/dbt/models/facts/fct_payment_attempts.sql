select
    event_id as payment_event_id,
    order_id,
    customer_id,
    product_id,
    region,
    event_timestamp as attempted_at,
    amount,
    currency,
    payment_provider,
    event_type = 'payment_processed' as is_successful,
    processing_latency_ms
from {{ ref('stg_stream_events') }}
where event_type in ('payment_processed', 'payment_failed')
