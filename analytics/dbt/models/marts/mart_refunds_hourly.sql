select
    date_trunc('hour', requested_at) as metric_hour,
    region,
    count(*) as refund_request_count,
    sum(amount) as requested_refund_amount
from {{ ref('fct_refund_requests') }}
group by 1, 2
