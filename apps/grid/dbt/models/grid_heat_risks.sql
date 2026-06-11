{{ config(
    materialized='incremental',
    unique_key=['date', 'dso_id', 'line_name', 'municipality', 'conductor_type', 'length_m'],
    schema='gold'
) }}

{#
    Heat risk per MT underground cable segment.

    Replaces: grid_heat_risks_linee + grid_heat_risks (old).

    Heat risk applies to underground cables only — buried conductors
    are sensitive to soil temperature / heat stress.
    Overhead lines are excluded.

    Source: silver_grid_ac_line_segment WHERE conductor_type = 'underground_cable'.
    Weather: om_heat_risk (spatial join ≤ 5 km, nearest station per segment per date).
    Date range: today + 2 days ahead.
#}

{% set seg  = source('grid_silver', 'silver_grid_ac_line_segment') %}
{% set heat = source('om_heat', 'om_heat_risk') %}

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
    from {{ heat }} h
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
        h.date,
        h.heat_risk_tier,
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

normalized as (

    select
        *,
        CASE heat_risk_tier
            WHEN 'RED'    THEN 'ALERT'
            WHEN 'ORANGE' THEN 'WARNING'
            WHEN 'GREEN'  THEN 'NORMAL'
            ELSE heat_risk_tier
        END as risk_tier
    from ranked

),

colored as (

    select
        *,
        {{ grid_risk_color('risk_tier') }} as risk_color_hex
    from normalized

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

    date,
    risk_tier             as risk_level,
    temp_max_c,
    p90_threshold,
    consecutive_heat_days,
    elevation_m,
    altitude_band,
    forecast_model,
    risk_color_hex

from colored
where rn = 1
