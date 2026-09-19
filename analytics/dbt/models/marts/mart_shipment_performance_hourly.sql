select
    date_trunc('hour', occurred_at) as metric_hour,
    region,
    count(*) filter (where is_created) as shipment_created_count,
    count(*) filter (where is_delay_signal) as shipment_delay_signal_count,
    count(*) filter (where is_delay_signal)::double precision
        / nullif(count(*) filter (where is_created), 0) as delay_signals_per_created_shipment
from {{ ref('fct_shipments') }}
group by 1, 2
