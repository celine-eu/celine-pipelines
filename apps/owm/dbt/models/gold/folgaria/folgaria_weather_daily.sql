-- models/gold/folgaria/folgaria_weather_daily.sql
{{ config(
    materialized='incremental',
    schema='gold',
    unique_key=['synced_at', 'dt_lat_lon'],
    incremental_strategy='merge',
    alias='folgaria_weather_daily',
    description="Daily weather forecast data from OpenWeather API for the Folgaria area"
) }}

with source as (
    select *
    from {{ ref('weather_daily') }}
),

filtered as (
    select
        synced_at,
        lat,
        lon,
        location_id,
        ts,
        pop,
        uvi,
        rain,
        clouds,
        sunrise,
        sunset,
        humidity,
        pressure,
        wind_deg,
        dew_point,
        wind_gust,
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
        weather_main,
        weather_description,
        summary,
        moonrise,
        moonset,
        dt_lat_lon
    from source
    where lower(location_id) like '%folgaria%'
)

select * from filtered