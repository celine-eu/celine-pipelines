{{ config(
    materialized='incremental',
    unique_key=['date', 'dso_id', 'line_name', 'municipality', 'conductor_type', 'length_m'],
    on_schema_change='append_new_columns',
    schema='gold'
) }}

{#
    Heat risk per MT underground cable segment.

    Heat risk applies to underground cables only: buried conductors are the
    ones the ground temperature derates. Overhead lines are excluded.

    Two axes, not one. The weather side is grid_heat_status (soil temperature
    crossed with the air heat tier per Open-Meteo point); the asset side is the
    thermal tier of the cable, the margin between its peak conductor
    temperature and the rating of its insulation. The matrix lives in the
    grid_heat_matrix macro:

        heat_status | tier low / mid | tier high
        ------------+----------------+----------
        GREEN       | NORMAL         | NORMAL
        ORANGE      | NORMAL         | WARNING
        RED         | WARNING        | ALERT

    Arcs the thermal model does not cover read tier 'low' (thermal_modelled
    false): they behave exactly as the air-only model did, so coverage gaps
    never invent risk.

    Source: silver_grid_ac_line_segment WHERE conductor_type = 'underground_cable'.
    Weather: grid_heat_status (nearest point within 5 km per segment per date).
    Date range: today + 2 days ahead.
#}

{% set seg = source('grid_silver', 'silver_grid_ac_line_segment') %}

with date_range as (

    select generate_series(
        {% if var('start_date', none) is not none %}
            '{{ var("start_date") }}'::date,
        {% else %}
            CURRENT_DATE,
        {% endif %}
        CURRENT_DATE + interval '2 days',
        interval '1 day'
    )::date as date

),

heat_latest as (

    select h.*
    from {{ ref('grid_heat_status') }} h
    join date_range d on h.date = d.date

),

with_dist as (

    select
        s.dso_id,
        s.line_name,
        s.conductor_type,
        s.parent_substation_name,
        s.operational_unit,
        s.feeder_id,
        s.municipality,
        s.length_m,
        s.geom,

        coalesce(s.thermal_tier, 'low')     as thermal_tier,
        s.thermal_tier is not null          as thermal_modelled,
        s.thermal_margin_c,
        s.thermal_theta_max_c,
        s.thermal_insulation,

        h.date,
        h.heat_status,
        h.soil_status,
        h.soil7_mean_c,
        h.soil7_p90_c,
        h.soil_asof_date,
        h.air_heat_tier,
        h.temp_max_c,
        h.p90_threshold,
        h.consecutive_heat_days,
        h.elevation_m,
        h.altitude_band,
        h.forecast_model,
        ST_Distance(
            s.geom,
            ST_Transform(h.geoposition::geometry, 32632)
        ) as dist_m
    from {{ seg }} s
    left join heat_latest h
        on ST_DWithin(ST_Transform(h.geoposition::geometry, 32632), s.geom, 5000)
    where s.conductor_type = 'underground_cable'

),

ranked as (

    select
        *,
        row_number() over (
            partition by date, line_name, municipality, conductor_type, length_m
            order by dist_m asc nulls last
        ) as rn
    from with_dist

),

leveled as (

    select
        *,
        {{ grid_heat_matrix('heat_status', 'thermal_tier') }}    as risk_tier,
        {{ grid_heat_escalated('heat_status', 'thermal_tier') }} as escalated_by_thermal
    from ranked

),

colored as (

    select
        *,
        {{ grid_risk_color('risk_tier') }} as risk_color_hex
    from leveled

)

select
    dso_id,
    line_name,
    conductor_type,
    parent_substation_name,
    operational_unit,
    feeder_id,
    municipality,
    length_m,

    thermal_tier,
    thermal_modelled,
    thermal_margin_c,
    thermal_theta_max_c,
    thermal_insulation,

    date,
    risk_tier             as risk_level,
    escalated_by_thermal,

    heat_status,
    soil_status,
    soil7_mean_c,
    soil7_p90_c,
    soil_asof_date,
    air_heat_tier,

    temp_max_c,
    p90_threshold,
    consecutive_heat_days,
    elevation_m,
    altitude_band,
    forecast_model,
    risk_color_hex

from colored
where rn = 1
