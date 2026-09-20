select
    date_trunc('hour', attempted_at) as metric_hour,
    region,
    count(*) as successful_payment_count,
    sum(amount) as successful_revenue,
    avg(amount) as average_successful_payment
from {{ ref('fct_payment_attempts') }}
where is_successful
group by 1, 2
