{{ config(
    materialized         = 'incremental',
    unique_key           = ['location_id', 'forecast_at'],
    incremental_strategy = 'merge'
) }}

with locations as (
    select * from {{ ref('weather_locations') }}
),

ranked as (
    select
        loc.location_id,
        src.provider,
        src.forecast_at,
        src.temperature_c,
        src.humidity_pct,
        src.wind_speed_ms,
        src.wind_gust_ms,
        src.wind_direction_deg,
        src.precipitation_mm,
        src.precipitation_probability_pct,
        src.cloud_cover_pct,
        src.sky_condition,
        src.pressure_hpa,
        src.snow_cm,
        row_number() over (
            partition by loc.location_id, src.forecast_at
            order by (abs(src.lat - loc.lat) + abs(src.lon - loc.lon))
        ) as rn
    from {{ ref('stg_weather_forecast_hourly') }} src
    join locations loc
        on abs(src.lat - loc.lat) < 0.1
        and abs(src.lon - loc.lon) < 0.1
)

select
    location_id,
    provider,
    forecast_at,
    temperature_c,
    humidity_pct,
    wind_speed_ms,
    wind_gust_ms,
    wind_direction_deg,
    precipitation_mm,
    precipitation_probability_pct,
    cloud_cover_pct,
    sky_condition,
    pressure_hpa,
    snow_cm
from ranked
where rn = 1
