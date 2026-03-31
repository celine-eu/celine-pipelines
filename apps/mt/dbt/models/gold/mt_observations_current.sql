{{ config(materialized='table') }}

{#
    Latest observation per active station from the last hour.
    Rebuilt on every run — always reflects current conditions.
    Joins with station registry to expose name, elevation, and coordinates.
#}

with latest as (

    select
        station_code,
        observed_at,
        air_temperature_c,
        precipitation_mm,
        wind_speed_ms,
        wind_direction_deg,
        wind_gust_ms,
        global_radiation_wm2,
        relative_humidity_pct,
        snow_depth_cm,
        row_number() over (
            partition by station_code
            order by observed_at desc
        ) as rn
    from {{ ref('mt_station_observations') }}
    where observed_at >= now() - interval '1 hour'

)

select
    l.station_code,
    s.name          as station_name,
    s.shortname     as station_shortname,
    s.elevation_m,
    s.latitude,
    s.longitude,
    l.observed_at,
    l.air_temperature_c,
    l.precipitation_mm,
    l.wind_speed_ms,
    l.wind_direction_deg,
    l.wind_gust_ms,
    l.global_radiation_wm2,
    l.relative_humidity_pct,
    l.snow_depth_cm
from latest l
join {{ ref('mt_meteo_stations') }} s on s.code = l.station_code
where l.rn = 1
  and (s.active_until is null or s.active_until >= current_date)
