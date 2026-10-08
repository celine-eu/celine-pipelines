-- One row per (ts, community_id, substation_id) in both community tables: their merge
-- key. A second row for a key would be counted twice by every reader summing a
-- community's figures. Composite, so a singular test (this project has no dbt_utils).
{{ config(tags=['rec_it']) }}

select 'rec_virtual_consumption_15m' as model, ts, community_id, substation_id, count(*) as rows
from {{ ref('rec_virtual_consumption_15m') }}
group by ts, community_id, substation_id
having count(*) > 1

union all

select 'rec_virtual_consumption_hourly', ts, community_id, substation_id, count(*)
from {{ ref('rec_virtual_consumption_hourly') }}
group by ts, community_id, substation_id
having count(*) > 1
