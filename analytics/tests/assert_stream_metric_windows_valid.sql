select window_start, window_end, event_type, event_count, total_amount
from {{ ref('stg_stream_metrics_minute') }}
where
    window_end <> window_start + interval '1 minute'
    or event_count < 0
    or total_amount < 0
