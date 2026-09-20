select 'payment_health' as model_name, failure_rate as rate
from {{ ref('mart_payment_health_hourly') }}
where failure_rate not between 0 and 1

union all

select 'shipment_health', delayed_shipment_rate
from {{ ref('mart_shipment_health_hourly') }}
where delayed_shipment_rate not between 0 and 1

union all

select 'operations_payment', payment_failure_rate
from {{ ref('mart_operations_health_hourly') }}
where payment_failure_rate is not null and payment_failure_rate not between 0 and 1

union all

select 'operations_shipment', delayed_shipment_rate
from {{ ref('mart_operations_health_hourly') }}
where delayed_shipment_rate is not null and delayed_shipment_rate not between 0 and 1
