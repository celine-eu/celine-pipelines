-- models/gold/folgaria/folgaria_weather_current.sql
{{ config(
    materialized='incremental',
    schema='gold',
    unique_key='dt_lat_lon',
    incremental_strategy='merge',
    alias='folgaria_weather_current',
    description="Current weather conditions from OpenWeather API for the Folgaria area"
) }}

with source as (
    select *
    from {{ ref('weather_current') }}
),

filtered as (
    select
        synced_at,
        lat,
        lon,
        timezone,
        timezone_offset,
        location_id,
        ts,
        temp,
        humidity,
        pressure,
        uvi,
        clouds,
        wind_deg,
        sunrise,
        sunset,
        weather_main,
        weather_description,
        dt_lat_lon
    from source
    where lower(location_id) like '%folgaria%'
)

select * from filtered