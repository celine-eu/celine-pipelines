{{ config(materialized='table', schema='gold') }}

{#
    Unified CIM asset registry, geometry only, no risk properties.

    Combines ACLineSegment, Substation and thermal joint assets into a single
    table so the frontend can load network topology once and cache it
    independently from daily risk updates.

    segment_id hash includes municipality because the silver layer splits lines
    at administrative boundaries: each (line, conductor_type, municipality)
    triple is a distinct map shape with its own geometry.
    Sub-fragments within the same triple are merged via ST_Union; lengths are
    summed; is_vegetated_zone is true when any fragment intersects forest.

    For substations: MD5(dso_id, 'substation', asset_id), asset_id is already unique.
    For joints:      MD5(dso_id, 'joint', joint_id), the same expression the
                     joint rows of grid_risks mint, byte for byte.

    Tree-strike columns. On a line the tier is the worst of its fragments
    (that is what escalates the wind risk, see grid_wind_risks), so the map
    also carries the km of fragments per tier (strike_km_high/mid/low) and a
    length-weighted density (strike trees per km of the whole tratta, with the
    unforested fragments counting as zero), so a 6 km tratta with one short
    'high' fragment is not read as 6 km of 'high'.

    Thermal columns. On a line the tier is the worst of its arcs (a tratta is as
    exposed as its hottest cable), the margin and the rating are the minimum,
    and the insulation reported is the one of the tightest-margin arc, so the
    panel names the class that is actually close to its limit. On a joint they
    come straight from the physical model, and a joint the model did not tier
    reads 'unmodelled' rather than NULL: it is on the map, it is not assessed.
    Substations carry NULL thermal columns.
#}

with lines as (

    select *
    from {{ source('grid_silver', 'silver_grid_ac_line_segment') }}

),

substations as (

    select *
    from {{ source('grid_silver', 'silver_grid_substation') }}

),

joints as (

    select *
    from {{ source('grid_silver', 'silver_grid_geo_thermal_joints') }}

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
        case
            when bool_or(strike_tree_tier = 'high') then 'high'
            when bool_or(strike_tree_tier = 'mid')  then 'mid'
            when bool_or(strike_tree_tier = 'low')  then 'low'
        end                                 as strike_tree_tier,
        max(strike_tree_multiplier)         as strike_tree_multiplier,
        case when sum(length_m) > 0 and bool_or(strike_tree_tier is not null)
             then coalesce(sum(strike_density_per_km * length_m), 0) / sum(length_m)
        end                                 as strike_density_per_km,
        coalesce(sum(length_m) filter (where strike_tree_tier = 'high'), 0) / 1000.0
                                            as strike_km_high,
        coalesce(sum(length_m) filter (where strike_tree_tier = 'mid'),  0) / 1000.0
                                            as strike_km_mid,
        coalesce(sum(length_m) filter (where strike_tree_tier = 'low'),  0) / 1000.0
                                            as strike_km_low,
        case
            when bool_or(thermal_tier = 'high') then 'high'
            when bool_or(thermal_tier = 'mid')  then 'mid'
            when bool_or(thermal_tier = 'low')  then 'low'
        end                                 as thermal_tier,
        min(thermal_margin_c)               as thermal_margin_c,
        min(thermal_theta_max_c)            as thermal_theta_max_c,
        (array_agg(thermal_insulation order by thermal_margin_c asc nulls last)
         filter (where thermal_insulation is not null))[1]
                                            as thermal_insulation,
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
    strike_tree_tier,
    strike_tree_multiplier,
    strike_density_per_km,
    strike_km_high,
    strike_km_mid,
    strike_km_low,
    thermal_tier,
    thermal_margin_c,
    thermal_theta_max_c,
    thermal_insulation,
    null::boolean           as is_asphalt,
    null::int               as anno_posa,
    null::text              as technology,
    null::float             as m_r_critico,
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
            'municipality',        municipality,
            'strike_tree_tier',       strike_tree_tier,
            'strike_tree_multiplier', strike_tree_multiplier,
            'strike_density_per_km',  strike_density_per_km,
            'strike_km_high',         strike_km_high,
            'strike_km_mid',          strike_km_mid,
            'strike_km_low',          strike_km_low,
            'thermal_tier',           thermal_tier,
            'thermal_margin_c',       thermal_margin_c,
            'thermal_theta_max_c',    thermal_theta_max_c,
            'thermal_insulation',     thermal_insulation
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
    null::text              as strike_tree_tier,
    null::float             as strike_tree_multiplier,
    null::float             as strike_density_per_km,
    null::float             as strike_km_high,
    null::float             as strike_km_mid,
    null::float             as strike_km_low,
    null::text              as thermal_tier,
    null::float             as thermal_margin_c,
    null::float             as thermal_theta_max_c,
    null::text              as thermal_insulation,
    null::boolean           as is_asphalt,
    null::int               as anno_posa,
    null::text              as technology,
    null::float             as m_r_critico,
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

union all

select
    md5(dso_id || '|' || 'joint' || '|' || joint_id::text) as segment_id,
    dso_id,
    'joint'                 as asset_type,
    joint_id::text          as asset_key,
    null::text              as line_name,
    null::text              as operational_unit,
    comune                  as municipality,
    null::text              as conductor_type,
    null::text              as parent_substation_name,
    null::text              as feeder_id,
    null::float             as length_m,
    null::boolean           as is_vegetated_zone,
    null::text              as strike_tree_tier,
    null::float             as strike_tree_multiplier,
    null::float             as strike_density_per_km,
    null::float             as strike_km_high,
    null::float             as strike_km_mid,
    null::float             as strike_km_low,
    coalesce(joint_tier, 'unmodelled')  as thermal_tier,
    margin_min_c            as thermal_margin_c,
    theta_max_c             as thermal_theta_max_c,
    insulation_class        as thermal_insulation,
    is_asphalt,
    anno_posa,
    {#- technology is text in the source contract; the cast plus trim is a
       no-op on text and strips the JSON quoting on a jsonb fixture. -#}
    trim(both '"' from technology::text) as technology,
    m_r_critico,
    null::text              as voltage_class,
    null::text              as label,
    null::text              as label_id,
    'Giunto ' || joint_id::text         as name,
    geom,
    {{ grid_geojson_feature(
        geom_col         = 'geom',
        geometry_type    = 'point',
        extra_props_expr = "json_build_object(
            'segment_id',          md5(dso_id || '|' || 'joint' || '|' || joint_id::text),
            'asset_type',          'joint',
            'joint_id',            joint_id,
            'name',                'Giunto ' || joint_id::text,
            'municipality',        comune,
            'thermal_tier',        coalesce(joint_tier, 'unmodelled'),
            'thermal_insulation',  insulation_class,
            'anno_posa',           anno_posa,
            'technology',          trim(both '\"' from technology::text),
            'thermal_margin_c',    margin_min_c,
            'm_r_critico',         m_r_critico,
            'is_asphalt',          is_asphalt
        )"
    ) }} as feature_geojson

from joints
