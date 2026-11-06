-- Fair gamification: Level and Growth points live in [0, 100], Actions in [0, cap],
-- and weekly_points is their sum. Returns the rows that break any of these.
select device_id, week_start
from {{ ref('rec_fair_weekly_scores') }}
where level_points < 0 or level_points > 100
   or growth_points < 0 or growth_points > 100
   or actions_points < 0 or actions_points > {{ var('fair_actions_weekly_cap') }}
   or abs(weekly_points - (coalesce(level_points, 0) + coalesce(growth_points, 0) + actions_points)) > 1e-6
