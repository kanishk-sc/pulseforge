with events as (
    select * from {{ ref('stg_stream_events') }}
),
orders as (
    select distinct order_id from {{ ref('fct_orders') }}
)
select
    event_hour as metric_hour,
    region,
    count(*) filter (
        where event_type in ('payment_processed', 'payment_failed')
          and order_id not in (select order_id from orders)
    ) as orphan_payment_attempt_count,
    count(*) filter (
        where event_type in ('shipment_created', 'shipment_delayed')
          and order_id not in (select order_id from orders)
    ) as orphan_shipment_event_count,
    max(processed_at) as measured_at
from events
group by 1, 2
