{{ config(materialized='table', schema='gold') }}

{#
    Tile-to-span mapping for progressive loading of the tree-strike overlay,
    on the same per-operator 5 km grid (and tile ids) as grid_tiles.
#}

select
    tg.dso_id,
    tg.tile_id,
    tg.tile_x,
    tg.tile_y,
    sp.span_id
from {{ ref('grid_tile_grid') }} tg
inner join {{ ref('grid_tree_strike_spans') }} sp
    on  sp.dso_id = tg.dso_id
    and ST_Intersects(tg.tile_geom, sp.geom)
