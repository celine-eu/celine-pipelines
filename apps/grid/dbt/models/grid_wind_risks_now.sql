{{ config(
    materialized='incremental',
    unique_key=['date', 'dso_id', 'line_name', 'municipality', 'conductor_type', 'length_m'],
    schema='gold',
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }}{% endif %}"
) }}

{#
    Wind risk per MT overhead line segment from real-time observations (nowcasting).

    Same logic as grid_wind_risks but sourced from om_obs_15min
    instead of om_wind_gusts forecasts.

    Source: silver_grid_ac_line_segment (conductor_type != underground_cable).
    Observations: om_obs_15min (spatial join ≤ 5 km, nearest station, most recent).
    Gust excess thresholds: WARNING >= 7.62 m/s, ALERT >= 12.46 m/s.

    Escalation: WARNING → ALERT when strike_tree_tier = 'high' (tree-strike analysis); NORMAL never escalates.
#}

{% set seg = source('grid_silver', 'silver_grid_ac_line_segment') %}
{% set obs = source('om_obs', 'om_obs_15min') %}

with recent_obs as (

    select distinct on (lat, lon)
        lat,
        lon,
        geoposition,
        wind_speed_ms,
        wind_gusts_ms,
        datetime as observed_at
    from {{ obs }}
    where datetime >= now() - interval '3 hours'
    order by lat, lon, datetime desc

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
        o.observed_at,
        o.wind_speed_ms,
        o.wind_gusts_ms,
        (o.wind_gusts_ms - o.wind_speed_ms) as gust_excess,
        ST_Distance(
            s.geom,
            ST_Transform(o.geoposition::geometry, 32632)
        ) as dist_m
    from {{ seg }} s
    left join recent_obs o
        on ST_DWithin(ST_Transform(o.geoposition::geometry, 32632), s.geom, 5000)
    where s.conductor_type != 'underground_cable'

),

ranked as (

    select
        *,
        row_number() over (
            partition by line_name, municipality, conductor_type, length_m
            order by dist_m asc nulls last
        ) as rn
    from with_dist

),

classified as (

    select
        *,
        {{ grid_wind_risk_level('wind_gusts_ms', 'wind_speed_ms') }} as risk_level
    from ranked
    where rn = 1

),

escalated as (

    select
        *,
        case
            when risk_level = 'WARNING' and strike_tree_tier = 'high'
            then 'ALERT'
            else risk_level
        end as risk_level_escalated,
        coalesce(
            risk_level = 'WARNING' and strike_tree_tier = 'high',
            false
        ) as escalated_by_tree_strike
    from classified

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

    current_date       as date,
    risk_level_escalated as risk_level,
    escalated_by_tree_strike,
    gust_excess,
    wind_speed_ms      as wind_speed_max,
    wind_gusts_ms      as wind_gusts_max,
    {{ grid_risk_color('risk_level_escalated') }} as risk_color_hex,
    observed_at

from escalated
where risk_level_escalated != 'NORMAL'
