-- Contract v1 has no shipment ID. Only a single creation per order supports attribution.
select order_id, count(*) as creation_count
from {{ ref('stg_stream_events') }}
where event_type = 'shipment_created'
group by order_id
having count(*) > 1
