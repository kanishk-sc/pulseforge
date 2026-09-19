select
    date_trunc('hour', attempted_at) as metric_hour,
    region,
    count(*) as payment_attempt_count,
    count(*) filter (where not is_successful) as failed_payment_count,
    count(*) filter (where is_successful) as successful_payment_count,
    count(*) filter (where not is_successful)::double precision / nullif(count(*), 0)
        as payment_failure_rate
from {{ ref('fct_payment_attempts') }}
group by 1, 2
