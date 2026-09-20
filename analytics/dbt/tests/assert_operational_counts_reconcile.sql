select *
from {{ ref('mart_operational_health_hourly') }}
where event_count < order_count
   or event_count < payment_attempt_count
   or failed_payment_count > payment_attempt_count
