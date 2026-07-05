{{ config(
    materialized         = 'incremental',
    unique_key           = ['location_id', 'forecast_date'],
    incremental_strategy = 'merge'
) }}

with locations as (
    select * from {{ ref('weather_locations') }}
),

ranked as (
    select
        loc.location_id,
        src.provider,
        src.forecast_date,
        src.temperature_c,
        src.temperature_min_c,
        src.temperature_max_c,
        src.precipitation_mm,
        src.precipitation_probability_pct,
        src.wind_speed_ms,
        src.wind_gust_ms,
        src.wind_direction_deg,
        src.cloud_cover_pct,
        src.sky_condition,
        src.snow_cm,
        src.sunrise,
        src.sunset,
        row_number() over (
            partition by loc.location_id, src.forecast_date
            order by (abs(src.lat - loc.lat) + abs(src.lon - loc.lon))
        ) as rn
    from {{ ref('stg_weather_forecast_daily') }} src
    join locations loc
        on abs(src.lat - loc.lat) < 0.1
        and abs(src.lon - loc.lon) < 0.1
)

select
    location_id,
    provider,
    forecast_date,
    temperature_c,
    temperature_min_c,
    temperature_max_c,
    precipitation_mm,
    precipitation_probability_pct,
    wind_speed_ms,
    wind_gust_ms,
    wind_direction_deg,
    cloud_cover_pct,
    sky_condition,
    snow_cm,
    sunrise,
    sunset
from ranked
where rn = 1
