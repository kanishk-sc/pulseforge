select
    event_id as refund_event_id,
    order_id,
    customer_id,
    product_id,
    region,
    event_timestamp as requested_at,
    amount,
    currency,
    processing_latency_ms
from {{ ref('stg_stream_events') }}
where event_type = 'refund_requested'
