-- Streak multiplier must be in [1.0, 1.5].
select device_id, multiplier
from {{ ref('rec_device_streaks') }}
where multiplier < 1.0 or multiplier > 1.5
