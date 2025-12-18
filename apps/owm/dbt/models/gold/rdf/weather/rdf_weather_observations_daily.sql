{{ config(
    materialized='view',
    schema='gold',
    alias='weather_observations_daily'
) }}

SELECT
    location_id,
    ts AS result_time,
    temp_day,
    temp_min,
    temp_max,
    temp_night,
    temp_eve,
    temp_morn,
    feels_like_day,
    feels_like_night,
    feels_like_eve,
    feels_like_morn,
    humidity,
    pressure,
    uvi,
    rain,
    clouds,
    wind_deg,
    wind_gust,
    weather_description,
    summary
FROM {{ ref("weather_daily") }}