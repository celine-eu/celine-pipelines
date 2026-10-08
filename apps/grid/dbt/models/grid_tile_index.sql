{{ config(materialized='view', schema='gold') }}

select
    dso_id,
    tile_id,
    tile_x,
    tile_y,
    tile_bbox_geojson,
    count(*) as segment_count
from {{ ref('grid_tiles') }}
group by dso_id, tile_id, tile_x, tile_y, tile_bbox_geojson
order by dso_id, tile_y, tile_x
