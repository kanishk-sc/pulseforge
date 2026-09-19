select
    product_id,
    min(event_timestamp) as first_seen_at,
    max(event_timestamp) as last_seen_at,
    count(*) as observed_event_count,
    count(distinct order_id) filter (where order_id is not null) as observed_order_count
from {{ ref('stg_stream_events') }}
where product_id is not null
group by product_id
