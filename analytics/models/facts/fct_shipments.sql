{{
    config(
        materialized='incremental',
        unique_key='source_event_id',
        incremental_strategy='delete+insert'
    )
}}

with shipment_creations as (
    select *
    from {{ ref('stg_stream_events') }}
    where event_type = 'shipment_created'
),

delay_events as (
    select
        order_id,
        count(*)::bigint as delay_event_count,
        min(event_ts) as first_delayed_at,
        max(event_ts) as last_delayed_at,
        (array_agg(event_id order by event_ts, event_id))[1] as first_delay_source_event_id,
        array_agg(event_id order by event_ts, event_id) as delay_source_event_ids
    from {{ ref('stg_stream_events') }}
    where event_type = 'shipment_delayed'
    group by order_id
)

select
    md5(created.event_id::text) as shipment_key,
    created.event_id as source_event_id,
    created.event_ts as shipment_created_at,
    created.order_id,
    md5(created.customer_id) as customer_key,
    md5(created.product_id) as product_key,
    md5(created.region) as region_key,
    created.customer_id,
    created.product_id,
    created.region as region_code,
    created.shipment_provider,
    created.status,
    coalesce(delays.delay_event_count, 0)::bigint as delay_event_count,
    (coalesce(delays.delay_event_count, 0) > 0) as was_delayed,
    delays.first_delayed_at,
    delays.last_delayed_at,
    delays.first_delay_source_event_id,
    delays.delay_source_event_ids,
    created.metadata,
    created.source_topic,
    created.source_partition,
    created.source_offset,
    created.source_timestamp,
    created.ingested_at
from shipment_creations as created
left join delay_events as delays using (order_id)
