select
    event_id as order_event_id,
    order_id,
    customer_id,
    product_id,
    region,
    event_timestamp as ordered_at,
    amount as order_amount,
    currency,
    processing_latency_ms
from {{ ref('stg_stream_events') }}
where event_type = 'order_created'
