{{ config(materialized='table') }}

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
            order by (abs(src.lat - loc.lat) + abs(src.lon - loc.lon))
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
