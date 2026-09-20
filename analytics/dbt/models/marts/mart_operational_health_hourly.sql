select
    event_hour as metric_hour,
    region,
    count(*) as event_count,
    count(*) filter (where event_type = 'order_created') as order_count,
    count(*) filter (where event_type in ('payment_processed', 'payment_failed'))
        as payment_attempt_count,
    count(*) filter (where event_type = 'payment_failed') as failed_payment_count,
    coalesce(sum(amount) filter (where event_type = 'payment_processed'), 0)
        as successful_revenue,
    count(*) filter (where event_type = 'shipment_created') as shipment_created_count,
    count(*) filter (where event_type = 'shipment_delayed') as shipment_delay_signal_count,
    count(*) filter (where event_type = 'refund_requested') as refund_request_count,
    avg(processing_latency_ms) as average_processing_latency_ms,
    max(processed_at) as warehouse_updated_at
from {{ ref('stg_stream_events') }}
group by 1, 2
