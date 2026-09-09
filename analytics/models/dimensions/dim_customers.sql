select
    md5(customer_id) as customer_key,
    customer_id,
    min(event_ts) as first_seen_at,
    max(event_ts) as last_seen_at,
    count(*)::bigint as source_event_count
from {{ ref('stg_stream_events') }}
where customer_id is not null
group by customer_id
