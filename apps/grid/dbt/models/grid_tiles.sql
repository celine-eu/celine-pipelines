{{ config(materialized='table', schema='gold') }}

{#
    Spatial tile index for progressive loading of grid shapes.

    Assigns every segment of grid_shapes to each 5 km tile of grid_tile_grid it
    intersects. Segments that span tile boundaries appear in multiple tiles —
    the frontend deduplicates by segment_id.
#}

select
    tg.tile_id,
    tg.tile_x,
    tg.tile_y,
    s.segment_id,
    tg.tile_bbox_geojson
from {{ ref('grid_tile_grid') }} tg
inner join {{ ref('grid_shapes') }} s
    on ST_Intersects(tg.tile_geom, s.geom)
