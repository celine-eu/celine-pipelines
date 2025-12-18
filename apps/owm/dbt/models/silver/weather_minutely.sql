{{ config(
    materialized='incremental',
    schema='silver',
    unique_key='dt_lat_lon',
    incremental_strategy='merge',
    alias='weather_minutely',
    description="Minutely weather forecast data from OpenWeather API, focusing on precipitation forecasts, normalized and stored incrementally"
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
        m as minute_record
    from source s,
         unnest(s.minutely) as m
),

parsed as (
    select
        synced_at,
        lat,
        lon,
        location_id,
        to_timestamp((minute_record ->> 'dt')::bigint) as dt,  -- Convert to UTC datetime
        (minute_record ->> 'precipitation')::float as precipitation,
        -- synthetic unique key
        concat((minute_record ->> 'dt')::text, '_', lat::text, '_', lon::text) as dt_lat_lon
    from exploded
)

select * from parsed

{% if is_incremental() %}
where dt_lat_lon not in (select dt_lat_lon from {{ this }})
{% endif %}