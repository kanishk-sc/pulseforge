{{
    config(
        materialized='incremental',
        unique_key='source_event_id',
        incremental_strategy='delete+insert'
    )
}}

select
    md5(event_id::text) as order_event_key,
    event_id as source_event_id,
    event_ts as order_created_at,
    order_id,
    md5(customer_id) as customer_key,
    md5(product_id) as product_key,
    md5(region) as region_key,
    customer_id,
    product_id,
    region as region_code,
    amount as order_amount,
    currency,
    status,
    metadata,
    source_topic,
    source_partition,
    source_offset,
    source_timestamp,
    ingested_at
from {{ ref('stg_stream_events') }}
where event_type = 'order_created'
