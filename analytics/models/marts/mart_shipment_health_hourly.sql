select
    date_trunc('hour', shipment_created_at) as shipment_creation_hour_utc,
    region_key,
    region_code,
    count(*)::bigint as shipment_created_count,
    count(*) filter (where was_delayed)::bigint as delayed_shipment_count,
    sum(delay_event_count)::bigint as shipment_delay_event_count,
    (
        count(*) filter (where was_delayed)::numeric
        / nullif(count(*), 0)
    )::numeric(12, 6) as delayed_shipment_rate
from {{ ref('fct_shipments') }}
group by 1, 2, 3
