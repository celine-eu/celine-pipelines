{{ config(
    materialized='incremental',
    unique_key=['date', 'dso_id', 'line_name', 'municipality', 'conductor_type', 'length_m'],
    schema='gold',
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }}{% endif %}"
) }}

{#
    Heat risk per underground cable segment from real-time observations (nowcasting).

    Reads from the heat_daily_obs proxy table (provider-agnostic daily max
    temperature with elevation and altitude band per observation point).
    Applies P90 heat stress thresholds, computes consecutive heat days via
    gaps-and-islands, and spatial-joins to underground cable segments.

    Source: silver_grid_ac_line_segment (conductor_type = underground_cable).
    Observations: heat_daily_obs (7-day window for streak calculation).
    Heat season: May-Sep only.
    P90 thresholds: Crespi 1981-2010, by altitude band and month.
#}

{% set seg = source('grid_silver', 'silver_grid_ac_line_segment') %}
{% set obs = source('heat_obs', 'heat_daily_obs') %}

with daily_obs as (

    select
        date,
        lat,
        lon,
        ST_SetSRID(
            ST_MakePoint(lon::double precision, lat::double precision),
            4326
        )::geography as geoposition,
        elevation_m,
        altitude_band,
        temp_max_c,
        latest_obs
    from {{ obs }}
    where date >= current_date - interval '7 days'

),

with_threshold as (

    select
        *,
        {{ grid_heat_p90_threshold('altitude_band', 'date') }} as p90_threshold,
        case
            when extract(month from date) between 5 and 9
                 and temp_max_c > {{ grid_heat_p90_threshold('altitude_band', 'date') }}
            then true
            else false
        end as is_heat_stress
    from daily_obs

),

islands as (

    select
        *,
        date - (row_number() over (
            partition by lat, lon, is_heat_stress
            order by date
        ))::int as island_id
    from with_threshold

),

streaks as (

    select
        *,
        case
            when is_heat_stress then
                row_number() over (
                    partition by lat, lon, island_id, is_heat_stress
                    order by date
                )
            else 0
        end as consecutive_heat_days
    from islands

),

today_risk as (

    select
        date,
        lat,
        lon,
        geoposition,
        temp_max_c,
        p90_threshold,
        consecutive_heat_days,
        elevation_m,
        altitude_band,
        latest_obs as observed_at,
        case
            when not is_heat_stress then 'NORMAL'
            when consecutive_heat_days >= 3 then 'ALERT'
            else 'WARNING'
        end as risk_tier
    from streaks
    where date = current_date

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
        h.date,
        h.risk_tier,
        h.temp_max_c,
        h.p90_threshold,
        h.consecutive_heat_days,
        h.elevation_m,
        h.altitude_band,
        h.observed_at,
        ST_Distance(
            s.geom,
            ST_Transform(h.geoposition::geometry, 32632)
        ) as dist_m
    from {{ seg }} s
    left join today_risk h
        on ST_DWithin(ST_Transform(h.geoposition::geometry, 32632), s.geom, 15000)
    where s.conductor_type = 'underground_cable'

),

ranked as (

    select
        *,
        row_number() over (
            partition by line_name, municipality, conductor_type, length_m
            order by dist_m asc nulls last
        ) as rn
    from with_dist

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
    {{ grid_risk_color('risk_tier') }} as risk_color_hex,
    observed_at

from ranked
where rn = 1
  and risk_tier != 'NORMAL'
