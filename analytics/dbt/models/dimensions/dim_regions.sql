select
    region,
    min(event_timestamp) as first_seen_at,
    max(event_timestamp) as last_seen_at,
    count(*) as observed_event_count,
    count(distinct customer_id) as observed_customer_count
from {{ ref('stg_stream_events') }}
group by region
