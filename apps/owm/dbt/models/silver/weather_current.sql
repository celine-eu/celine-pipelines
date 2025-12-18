{{ config(
    materialized='incremental',
    schema='silver',
    unique_key='dt_lat_lon',
    incremental_strategy='merge',
    alias='weather_current',
    description="Current weather conditions from OpenWeather API, normalized and stored incrementally"
  )
}}

with source as (
    select *
    from {{ ref('stg_forecast_stream') }}
),

parsed as (
    select
        synced_at,
        lat,
        lon,
        timezone,
        timezone_offset,
        location_id,
        to_timestamp((current ->> 'dt')::bigint) as ts,
        (current ->> 'temp')::float      as temp,
        (current ->> 'humidity')::int    as humidity,
        (current ->> 'pressure')::int    as pressure,
        (current ->> 'uvi')::float       as uvi,
        (current ->> 'clouds')::int      as clouds,
        (current ->> 'wind_deg')::int    as wind_deg,
        to_timestamp((current ->> 'sunrise')::bigint)  as sunrise,
        to_timestamp((current ->> 'sunset')::bigint)   as sunset,
        (current -> 'weather' -> 0 ->> 'main') as weather_main,
        (current -> 'weather' -> 0 ->> 'description') as weather_description,
        -- synthetic unique key
        concat(current ->> 'dt', '_', lat::text, '_', lon::text) as dt_lat_lon
    from source
)

select * from parsed

{% if is_incremental() %}
where dt_lat_lon not in (select dt_lat_lon from {{ this }})
{% endif %}