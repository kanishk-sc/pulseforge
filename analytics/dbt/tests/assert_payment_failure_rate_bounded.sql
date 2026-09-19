select *
from {{ ref('mart_payment_failure_hourly') }}
where payment_failure_rate < 0 or payment_failure_rate > 1
