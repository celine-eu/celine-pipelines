-- estimated_kwh and reward_points_estimated must never be negative
select *
from {{ ref('rec_flexibility_windows') }}
where estimated_kwh < 0 or reward_points_estimated < 0
