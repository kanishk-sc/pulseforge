select
    date_trunc('hour', refund_requested_at) as refund_request_hour_utc,
    region_key,
    region_code,
    count(*)::bigint as refund_request_count,
    sum(requested_amount)::numeric(24, 2) as requested_amount
from {{ ref('fct_refund_requests') }}
group by 1, 2, 3
