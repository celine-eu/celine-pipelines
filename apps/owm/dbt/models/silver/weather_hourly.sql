{{ config(
    materialized='incremental',
    schema='silver',
    unique_key='dt_lat_lon',
    incremental_strategy='merge',
    alias='weather_hourly',
    description="Hourly weather forecast data from OpenWeather API, normalized and stored incrementally"
  )
}}

with source as (
    select *
    from {{ ref('stg_forecast_stream') }}
),

exploded as (
    select
        s.synced_at,
        s.lat,
        s.lon,
        s.location_id,
        h as hourly_record
    from source s,
         unnest(s.hourly) as h
),

parsed as (
    select
        synced_at,
        lat,
        lon,
        location_id,
        to_timestamp((hourly_record ->> 'dt')::bigint) as ts,
        (hourly_record ->> 'temp')::float         as temp,
        (hourly_record ->> 'humidity')::int       as humidity,
        (hourly_record ->> 'pressure')::int       as pressure,
        (hourly_record ->> 'uvi')::float          as uvi,
        (hourly_record ->> 'clouds')::int         as clouds,
        (hourly_record ->> 'wind_deg')::int       as wind_deg,
        (hourly_record -> 'weather' -> 0 ->> 'main') as weather_main,
        (hourly_record -> 'weather' -> 0 ->> 'description') as weather_description,
        -- synthetic unique key
        concat((hourly_record ->> 'dt')::text, '_', lat::text, '_', lon::text) as dt_lat_lon
    from exploded
)

select * from parsed


{% if is_incremental() %}
where dt_lat_lon not in (select dt_lat_lon from {{ this }})
{% endif %}
