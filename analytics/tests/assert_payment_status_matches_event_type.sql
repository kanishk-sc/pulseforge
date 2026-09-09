select source_event_id, event_type, status, payment_succeeded
from {{ ref('fct_payment_attempts') }}
where
    (event_type = 'payment_processed' and (status <> 'paid' or not payment_succeeded))
    or (event_type = 'payment_failed' and (status <> 'failed' or payment_succeeded))
