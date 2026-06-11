{{ config(materialized='view', schema='gold') }}

select
    tile_id,
    tile_x,
    tile_y,
    tile_bbox_geojson,
    count(*) as segment_count
from {{ ref('grid_tiles') }}
group by tile_id, tile_x, tile_y, tile_bbox_geojson
order by tile_y, tile_x
