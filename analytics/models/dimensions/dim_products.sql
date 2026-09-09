select
    md5(product_id) as product_key,
    product_id,
    min(event_ts) as first_seen_at,
    max(event_ts) as last_seen_at,
    count(*)::bigint as source_event_count
from {{ ref('stg_stream_events') }}
where product_id is not null
group by product_id
