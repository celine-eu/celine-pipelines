-- Fair gamification: scores exist only for completed ISO weeks (Monday + 7 days ≤ today, Europe/Rome).
select device_id, week_start
from {{ ref('rec_fair_weekly_scores') }}
where week_start + 7 > (now() at time zone '{{ var("fair_tz") }}')::date
