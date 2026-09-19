select
    event_id as shipment_event_id,
    order_id,
    customer_id,
    product_id,
    region,
    event_timestamp as occurred_at,
    shipment_provider,
    event_type = 'shipment_created' as is_created,
    event_type = 'shipment_delayed' as is_delay_signal,
    processing_latency_ms
from {{ ref('stg_stream_events') }}
where event_type in ('shipment_created', 'shipment_delayed')
