{{ config(materialized='table', schema='gold') }}

{#
    Spatial tile index for progressive loading of grid shapes.

    Overlays a 5 km x 5 km UTM grid (EPSG:32632) on the bounding extent of
    grid_shapes, then assigns every segment to each tile it intersects.
    Segments that span tile boundaries appear in multiple tiles — the frontend
    deduplicates by segment_id.

    Tile coordinates (tile_x, tile_y) are 0-based from the south-west corner
    of the extent.  tile_id encodes them as "tile_{x}_{y}".
#}

{% set tile_size = 5000 %}

with extent as (

    select
        floor(ST_XMin(ST_Extent(geom)) / {{ tile_size }}.0)::int * {{ tile_size }} as x_min,
        floor(ST_YMin(ST_Extent(geom)) / {{ tile_size }}.0)::int * {{ tile_size }} as y_min,
        ceil(ST_XMax(ST_Extent(geom))  / {{ tile_size }}.0)::int * {{ tile_size }} as x_max,
        ceil(ST_YMax(ST_Extent(geom))  / {{ tile_size }}.0)::int * {{ tile_size }} as y_max
    from {{ ref('grid_shapes') }}

),

tile_grid as (

    select
        'tile_' || ((x.v - e.x_min) / {{ tile_size }})::int
             || '_' || ((y.v - e.y_min) / {{ tile_size }})::int   as tile_id,
        ((x.v - e.x_min) / {{ tile_size }})::int                  as tile_x,
        ((y.v - e.y_min) / {{ tile_size }})::int                  as tile_y,
        ST_SetSRID(
            ST_MakeEnvelope(x.v, y.v, x.v + {{ tile_size }}, y.v + {{ tile_size }}),
            32632
        ) as tile_geom
    from extent e
    cross join lateral generate_series(e.x_min, e.x_max - {{ tile_size }}, {{ tile_size }}) as x(v)
    cross join lateral generate_series(e.y_min, e.y_max - {{ tile_size }}, {{ tile_size }}) as y(v)

)

select
    tg.tile_id,
    tg.tile_x,
    tg.tile_y,
    s.segment_id,
    ST_AsGeoJSON(ST_Transform(tg.tile_geom, 4326))::jsonb as tile_bbox_geojson
from tile_grid tg
inner join {{ ref('grid_shapes') }} s
    on ST_Intersects(tg.tile_geom, s.geom)
