select *
from {{ ref('mart_revenue_hourly') }}
where successful_revenue < 0
