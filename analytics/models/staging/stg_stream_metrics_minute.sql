select
    window_start::timestamptz as window_start,
    window_end::timestamptz as window_end,
    event_type::text as event_type,
    event_count::bigint as event_count,
    total_amount::numeric(24, 2) as total_amount,
    updated_at::timestamptz as updated_at
from {{ source('streaming_warehouse', 'stream_metrics_minute') }}
