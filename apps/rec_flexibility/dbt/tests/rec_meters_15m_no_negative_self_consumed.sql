-- self_consumed_kw must never be negative (clipped in the model via greatest(..., 0)).
select device_id, ts, self_consumed_kw
from {{ ref('rec_meters_15m') }}
where self_consumed_kw < 0
