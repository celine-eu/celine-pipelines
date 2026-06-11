{{ config(materialized='table', schema='gold') }}

{#
    Map-ready GeoJSON output for secondary substations (CIM: Substation).
    Static infrastructure layer — no weather join.
    Source: silver_grid_substation.
#}

with base as (

    select *
    from {{ source('grid_silver', 'silver_grid_substation') }}

)

select
    dso_id,
    asset_id,
    name,
    label_id,
    label,
    line_name,
    feeder_id,
    parent_substation_name,
    operational_unit,
    municipality,
    geom,

    ST_X(ST_Transform(geom, 4326)) as longitude,
    ST_Y(ST_Transform(geom, 4326)) as latitude,

    {{ grid_geojson_feature(
        geom_col         = 'geom',
        geometry_type    = 'point',
        extra_props_expr = "json_build_object(
            'label',           coalesce(label, '') || ' — ' || municipality,
            'asset_id',        asset_id,
            'name',            name,
            'municipality',    municipality,
            'substation_name', parent_substation_name,
            'line_name',       line_name
        )"
    ) }} as feature_geojson

from base
