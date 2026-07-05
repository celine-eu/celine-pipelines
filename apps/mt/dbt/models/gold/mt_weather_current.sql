{{ config(
    materialized = 'table',
    alias        = 'weather__current'
) }}

{#
    Weather contract: current observations normalized to the shared schema.
    Uses the most recent observation per station (within 6h fallback window)
    regardless of exact ingestion timing. Joins with station registry for
    coordinates.
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
        row_number() over (
            partition by station_code
            order by observed_at desc
        ) as rn
    from {{ ref('mt_station_observations') }}
    where observed_at >= now() - interval '6 hours'
)

select
    'mt'::text                          as provider,
    l.station_code                      as station_id,
    s.name                              as station_name,
    s.latitude                          as lat,
    s.longitude                         as lon,
    l.observed_at,
    l.air_temperature_c                 as temperature_c,
    l.relative_humidity_pct             as humidity_pct,
    l.wind_speed_ms,
    l.wind_direction_deg::int           as wind_direction_deg,
    l.wind_gust_ms,
    l.precipitation_mm,
    null::float                         as cloud_cover_pct,
    null::text                          as sky_condition,
    null::float                         as pressure_hpa
from latest l
join {{ ref('mt_meteo_stations') }} s on s.code = l.station_code
where l.rn = 1
  and (s.active_until is null or s.active_until >= current_date)
