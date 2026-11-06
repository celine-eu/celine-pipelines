-- Fair gamification: a quarantined device-week never receives Level or Growth percentiles
-- (it is out of the boards and out of every reference group).
select s.device_id, s.week_start
from {{ ref('rec_fair_weekly_scores') }} s
join {{ ref('rec_fair_device_class') }} k using (device_id, week_start)
where k.is_quarantined and (s.level_pct is not null or s.growth_pct is not null)
