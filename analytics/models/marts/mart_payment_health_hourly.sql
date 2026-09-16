select
    date_trunc('hour', payment_event_ts, 'UTC') as payment_hour_utc,
    region_key,
    region_code,
    count(*)::bigint as payment_attempt_count,
    count(*) filter (where payment_succeeded)::bigint as successful_payment_count,
    count(*) filter (where not payment_succeeded)::bigint as failed_payment_count,
    (
        count(*) filter (where not payment_succeeded)::numeric
        / nullif(count(*), 0)
    )::numeric(12, 6) as failure_rate,
    sum(payment_amount)::numeric(24, 2) as attempted_amount,
    coalesce(sum(payment_amount) filter (where payment_succeeded), 0)::numeric(24, 2)
        as successful_payment_amount,
    coalesce(sum(payment_amount) filter (where not payment_succeeded), 0)::numeric(24, 2)
        as failed_payment_amount
from {{ ref('fct_payment_attempts') }}
group by 1, 2, 3
