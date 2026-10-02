{{ config(severity='warn') }}
-- The joint segment_id in grid_risks must be the one grid_shapes mints, or the
-- map panel opens on nothing. grid_shapes is rebuilt monthly and grid_risks
-- daily, so a miss can also be a stale shapes table: warn, do not fail the run.

select distinct
    r.segment_id,
    r.metrics ->> 'joint_id' as joint_id
from {{ ref('grid_risks') }} r
left join {{ ref('grid_shapes') }} s
    on s.segment_id = r.segment_id
where r.metrics ->> 'asset_type' = 'joint'
  and s.segment_id is null
