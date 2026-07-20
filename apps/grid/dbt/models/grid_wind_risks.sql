{{ config(
    materialized='incremental',
    unique_key=['date', 'dso_id', 'line_name', 'municipality', 'conductor_type', 'length_m'],
    schema='gold'
) }}

{#
    Wind risk per MT line segment (all overhead conductors).

    Replaces: grid_wind_risks_linee + grid_wind_risks (old).

    Source: silver_grid_ac_line_segment — all conductor types except
      underground_cable (wind risk is not relevant for buried cables).
    Weather: om_wind_gusts (spatial join ≤ 5 km, nearest station per segment per date).
    Date range: today + 2 days ahead.

    is_vegetated_zone = true marks overhead segments that pass through
    forested areas (derived from tratte boscate spatial annotation).
    Elevation fields are populated for vegetated segments only.

    Escalation: WARNING → ALERT when strike_tree_tier = 'high' (tree-strike analysis); NORMAL never escalates.
#}

{% set seg   = source('grid_silver', 'silver_grid_ac_line_segment') %}
{% set gusts = source('om_wind', 'om_wind_gusts') %}

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

gusts_latest as (

    select g.*
    from {{ gusts }} g
    join date_range d on g.date = d.date

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
        s.is_vegetated_zone,
        s.elevation_start_m,
        s.elevation_end_m,
        s.strike_tree_tier,
        s.strike_tree_multiplier,
        s.strike_density_per_km,
        s.geom,
        g.date,
        g.gust_excess_tier,
        g.gust_excess,
        g.wind_speed_max,
        g.wind_gusts_max,
        ST_Distance(
            s.geom,
            ST_Transform(g.geoposition::geometry, 32632)
        ) as dist_m
    from {{ seg }} s
    left join gusts_latest g
        on ST_DWithin(ST_Transform(g.geoposition::geometry, 32632), s.geom, 5000)
    where s.conductor_type != 'underground_cable'

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

escalated as (

    select
        *,
        case
            when gust_excess_tier = 'WARNING' and strike_tree_tier = 'high'
            then 'ALERT'
            else gust_excess_tier
        end as risk_level,
        coalesce(
            gust_excess_tier = 'WARNING' and strike_tree_tier = 'high',
            false
        ) as escalated_by_tree_strike
    from ranked

),

colored as (

    select
        *,
        {{ grid_risk_color('risk_level') }} as risk_color_hex
    from escalated

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
    is_vegetated_zone,
    elevation_start_m,
    elevation_end_m,
    strike_tree_tier,
    strike_tree_multiplier,
    strike_density_per_km,

    date,
    risk_level,
    escalated_by_tree_strike,
    gust_excess,
    wind_speed_max,
    wind_gusts_max,
    risk_color_hex

from colored
where rn = 1
