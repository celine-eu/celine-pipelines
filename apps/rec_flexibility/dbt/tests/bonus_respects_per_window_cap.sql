-- bonus_points_capped must never exceed bonus_points_raw, and the final bonus_points
-- must equal round(bonus_points_capped).
select _id, device_id, window_start, bonus_points, bonus_points_capped, bonus_points_raw
from {{ ref('rec_flexibility_bonus') }}
where bonus_points_capped > bonus_points_raw + 0.001
   or bonus_points > round(bonus_points_capped)::int + 0.001
