{{ config(materialized='table') }}

{#
    Ranking is lexicographic: horizontal distance first, then elevation
    difference to break ties between candidates sharing a coordinate, then
    location_id so the result is stable. See weather_forecast_hourly for the
    full reasoning — providers publish mountain massifs as a vertical profile at
    one representative lat/lon, and without the elevation term the winner among
    those bands was whichever row Postgres returned first.
#}

with locations as (
    select * from {{ ref('weather_locations') }}
),

ranked as (
    select
        loc.location_id,
        src.provider,
        src.station_id,
        src.station_name,
        src.observed_at,
        src.elevation_m,
        src.temperature_c,
        src.humidity_pct,
        src.wind_speed_ms,
        src.wind_direction_deg,
        src.wind_gust_ms,
        src.precipitation_mm,
        src.cloud_cover_pct,
        src.sky_condition,
        src.pressure_hpa,
        row_number() over (
            partition by loc.location_id
            order by
                (abs(src.lat - loc.lat) + abs(src.lon - loc.lon)),
                abs(src.elevation_m - loc.elevation_m) nulls last,
                src.station_id
        ) as rn
    from {{ ref('stg_weather_current') }} src
    cross join locations loc
    where abs(src.lat - loc.lat) < 0.15
      and abs(src.lon - loc.lon) < 0.15
)

select
    location_id,
    provider,
    station_id,
    station_name,
    observed_at,
    elevation_m,
    temperature_c,
    humidity_pct,
    wind_speed_ms,
    wind_direction_deg,
    wind_gust_ms,
    precipitation_mm,
    cloud_cover_pct,
    sky_condition,
    pressure_hpa
from ranked
where rn = 1
