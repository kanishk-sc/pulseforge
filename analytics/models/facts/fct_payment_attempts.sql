{{
    config(
        materialized='incremental',
        unique_key='source_event_id',
        incremental_strategy='delete+insert'
    )
}}

select
    md5(event_id::text) as payment_attempt_key,
    event_id as source_event_id,
    event_type,
    event_ts as payment_event_ts,
    order_id,
    md5(customer_id) as customer_key,
    md5(product_id) as product_key,
    md5(region) as region_key,
    customer_id,
    product_id,
    region as region_code,
    amount as payment_amount,
    currency,
    payment_provider,
    status,
    (event_type = 'payment_processed') as payment_succeeded,
    metadata,
    source_topic,
    source_partition,
    source_offset,
    source_timestamp,
    ingested_at
from {{ ref('stg_stream_events') }}
where event_type in ('payment_processed', 'payment_failed')
