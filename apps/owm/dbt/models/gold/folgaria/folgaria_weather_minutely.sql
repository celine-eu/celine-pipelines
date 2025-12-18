-- models/gold/folgaria/folgaria_weather_minutely.sql
{{ config(
    materialized='incremental',
    schema='gold',
    unique_key=['synced_at', 'dt_lat_lon'],
    incremental_strategy='merge',
    alias='folgaria_weather_minutely',
    description="Minutely weather forecast data from OpenWeather API for the Folgaria area"
) }}

with source as (
    select *
    from {{ ref('weather_minutely') }}
),

filtered as (
    select
        synced_at,
        lat,
        lon,
        location_id,
        dt,
        precipitation,
        dt_lat_lon
    from source
    where lower(location_id) like '%folgaria%'
)

select * from filtered