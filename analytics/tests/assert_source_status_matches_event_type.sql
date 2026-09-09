select event_id, event_type, status
from {{ ref('stg_stream_events') }}
where status <> case event_type
    when 'order_created' then 'created'
    when 'payment_processed' then 'paid'
    when 'payment_failed' then 'failed'
    when 'shipment_created' then 'shipped'
    when 'shipment_delayed' then 'delayed'
    when 'refund_requested' then 'requested'
    when 'inventory_updated' then 'updated'
    when 'customer_login' then 'authenticated'
end
