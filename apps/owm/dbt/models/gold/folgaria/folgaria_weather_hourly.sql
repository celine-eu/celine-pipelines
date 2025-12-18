-- models/gold/folgaria/folgaria_weather_hourly.sql
{{ config(
    materialized='incremental',
    schema='gold',
    unique_key=['synced_at', 'dt_lat_lon'],
    incremental_strategy='merge',
    alias='folgaria_weather_hourly',
    description="Hourly weather forecast data from OpenWeather API for the Folgaria area"
) }}

with source as (
    select *
    from {{ ref('weather_hourly') }}
),

filtered as (
    select
        synced_at,
        lat,
        lon,
        location_id,
        ts,
        temp,
        humidity,
        pressure,
        uvi,
        clouds,
        wind_deg,
        weather_main,
        weather_description,
        dt_lat_lon
    from source
    where lower(location_id) like '%folgaria%'
)

select * from filtered