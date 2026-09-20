select source_topic, source_partition, source_offset, count(*) as duplicate_count
from {{ ref('stg_stream_events') }}
group by source_topic, source_partition, source_offset
having count(*) > 1
