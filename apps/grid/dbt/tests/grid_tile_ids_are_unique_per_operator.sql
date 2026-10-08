-- tile_id is unique per operator: each operator has its own grid, so the same
-- tile_id may appear once for every dso_id and never twice for one.
select dso_id, tile_id, count(*) as n
from {{ ref('grid_tile_grid') }}
group by dso_id, tile_id
having count(*) > 1

union all

select dso_id, tile_id, count(*) as n
from {{ ref('grid_tile_index') }}
group by dso_id, tile_id
having count(*) > 1
