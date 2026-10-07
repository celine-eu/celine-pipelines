-- One row per (community_id, ts_date) in rec_co2_savings_community, its merge key.
-- A check on ts_date alone would fail as soon as two communities have figures for the
-- same day.
select community_id, ts_date, count(*) as rows
from {{ ref('rec_co2_savings_community') }}
group by community_id, ts_date
having count(*) > 1
