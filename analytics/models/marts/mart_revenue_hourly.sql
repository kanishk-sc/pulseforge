select
    date_trunc('hour', payment_event_ts) as revenue_hour_utc,
    region_key,
    region_code,
    count(*)::bigint as successful_payment_count,
    sum(payment_amount)::numeric(24, 2) as revenue_amount
from {{ ref('fct_payment_attempts') }}
where payment_succeeded
group by 1, 2, 3
