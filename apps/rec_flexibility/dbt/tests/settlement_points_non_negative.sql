-- Settlement points must never be negative.
select _id, device_id, ts, settlement_points
from {{ ref('rec_settlement_points') }}
where settlement_points < 0
