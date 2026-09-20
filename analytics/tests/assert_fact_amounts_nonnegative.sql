select 'orders' as fact_name, source_event_id, order_amount as amount
from {{ ref('fct_orders') }}
where order_amount < 0

union all

select 'payment_attempts', source_event_id, payment_amount
from {{ ref('fct_payment_attempts') }}
where payment_amount < 0

union all

select 'refund_requests', source_event_id, requested_amount
from {{ ref('fct_refund_requests') }}
where requested_amount < 0
