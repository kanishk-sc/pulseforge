with grain as (
    select date_trunc('hour', order_created_at, 'UTC') as metric_hour_utc, region_key, region_code
    from {{ ref('fct_orders') }}
    union
    select revenue_hour_utc, region_key, region_code from {{ ref('mart_revenue_hourly') }}
    union
    select payment_hour_utc, region_key, region_code
    from {{ ref('mart_payment_health_hourly') }}
    union
    select shipment_creation_hour_utc, region_key, region_code
    from {{ ref('mart_shipment_health_hourly') }}
    union
    select refund_request_hour_utc, region_key, region_code
    from {{ ref('mart_refunds_hourly') }}
),

orders as (
    select
        date_trunc('hour', order_created_at, 'UTC') as metric_hour_utc,
        region_key,
        count(*)::bigint as order_count
    from {{ ref('fct_orders') }}
    group by 1, 2
)

select
    grain.metric_hour_utc,
    grain.region_key,
    grain.region_code,
    coalesce(orders.order_count, 0)::bigint as order_count,
    coalesce(revenue.successful_payment_count, 0)::bigint as successful_payment_count,
    coalesce(revenue.revenue_amount, 0)::numeric(24, 2) as revenue_amount,
    coalesce(payments.payment_attempt_count, 0)::bigint as payment_attempt_count,
    coalesce(payments.failed_payment_count, 0)::bigint as failed_payment_count,
    payments.failure_rate as payment_failure_rate,
    coalesce(shipments.shipment_created_count, 0)::bigint as shipment_created_count,
    coalesce(shipments.delayed_shipment_count, 0)::bigint as delayed_shipment_count,
    coalesce(shipments.shipment_delay_event_count, 0)::bigint as shipment_delay_event_count,
    shipments.delayed_shipment_rate,
    coalesce(refunds.refund_request_count, 0)::bigint as refund_request_count,
    coalesce(refunds.requested_amount, 0)::numeric(24, 2) as refund_requested_amount,
    (
        coalesce(refunds.refund_request_count, 0)::numeric / nullif(orders.order_count, 0)
    )::numeric(12, 6) as refund_requests_per_order
from grain
left join orders using (metric_hour_utc, region_key)
left join {{ ref('mart_revenue_hourly') }} as revenue
    on grain.metric_hour_utc = revenue.revenue_hour_utc
    and grain.region_key = revenue.region_key
left join {{ ref('mart_payment_health_hourly') }} as payments
    on grain.metric_hour_utc = payments.payment_hour_utc
    and grain.region_key = payments.region_key
left join {{ ref('mart_shipment_health_hourly') }} as shipments
    on grain.metric_hour_utc = shipments.shipment_creation_hour_utc
    and grain.region_key = shipments.region_key
left join {{ ref('mart_refunds_hourly') }} as refunds
    on grain.metric_hour_utc = refunds.refund_request_hour_utc
    and grain.region_key = refunds.region_key
