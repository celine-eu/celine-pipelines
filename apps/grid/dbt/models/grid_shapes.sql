{{ config(materialized='table', schema='gold') }}

{#
    Unified CIM asset registry — geometry only, no risk properties.

    Combines ACLineSegment and Substation assets into a single table so the
    frontend can load network topology once and cache it independently from
    daily risk updates.

    segment_id hash includes municipality because the silver layer splits lines
    at administrative boundaries — each (line, conductor_type, municipality)
    triple is a distinct map shape with its own geometry.
    Sub-fragments within the same triple are merged via ST_Union; lengths are
    summed; is_vegetated_zone is true when any fragment intersects forest.

    For substations: MD5(dso_id, 'substation', asset_id) — asset_id is already unique.
#}

with lines as (

    select *
    from {{ source('grid_silver', 'silver_grid_ac_line_segment') }}

),

substations as (

    select *
    from {{ source('grid_silver', 'silver_grid_substation') }}

),

lines_agg as (

    select
        md5(
            dso_id || '|' || 'ac_line_segment' || '|' ||
            line_name || '|' || conductor_type || '|' || municipality
        )                                   as segment_id,
        dso_id,
        'ac_line_segment'                   as asset_type,
        line_name                           as asset_key,
        min(operational_unit)               as operational_unit,
        municipality,
        conductor_type,
        min(parent_substation_name)         as parent_substation_name,
        min(feeder_id)                      as feeder_id,
        sum(length_m)                       as length_m,
        bool_or(is_vegetated_zone)          as is_vegetated_zone,
        ST_Union(geom)                      as geom
    from lines
    group by
        dso_id, line_name, conductor_type, municipality

)

select
    segment_id,
    dso_id,
    asset_type,
    asset_key,
    asset_key                as line_name,
    operational_unit,
    municipality,
    conductor_type,
    parent_substation_name,
    feeder_id,
    length_m,
    is_vegetated_zone,
    null::text              as voltage_class,
    null::text              as label,
    null::text              as label_id,
    null::text              as name,
    geom,
    {{ grid_geojson_feature(
        geom_col         = 'geom',
        geometry_type    = 'line',
        extra_props_expr = "json_build_object(
            'segment_id',          segment_id,
            'asset_type',          'ac_line_segment',
            'line_name',           asset_key,
            'conductor_type',      conductor_type,
            'parent_substation_name', parent_substation_name,
            'operational_unit',    operational_unit,
            'municipality',        municipality
        )"
    ) }} as feature_geojson

from lines_agg

union all

select
    md5(dso_id || '|' || 'substation' || '|' || asset_id) as segment_id,
    dso_id,
    'substation'            as asset_type,
    asset_id                as asset_key,
    line_name,
    operational_unit,
    municipality,
    null::text              as conductor_type,
    parent_substation_name,
    feeder_id,
    null::float             as length_m,
    null::boolean           as is_vegetated_zone,
    voltage_class,
    label,
    label_id,
    name,
    geom,
    {{ grid_geojson_feature(
        geom_col         = 'geom',
        geometry_type    = 'point',
        extra_props_expr = "json_build_object(
            'segment_id',          md5(dso_id || '|' || 'substation' || '|' || asset_id),
            'asset_type',          'substation',
            'asset_id',            asset_id,
            'name',                name,
            'label',               coalesce(label, '') || ' — ' || municipality,
            'line_name',           line_name,
            'parent_substation_name', parent_substation_name,
            'operational_unit',    operational_unit,
            'municipality',        municipality
        )"
    ) }} as feature_geojson

from substations
