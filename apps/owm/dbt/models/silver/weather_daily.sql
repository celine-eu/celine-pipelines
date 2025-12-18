{{ config(
    materialized='incremental',
    schema='silver',
    unique_key='dt_lat_lon',
    incremental_strategy='merge',
    alias='weather_daily',
    description="Daily weather forecast data from OpenWeather API, normalized and stored incrementally"
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
        d as day_record
    from source s,
         unnest(s.daily) as d
),

parsed as (
    select
        synced_at,
        lat,
        lon,
        location_id,
        to_timestamp((day_record ->> 'dt')::bigint) as ts,
        (day_record ->> 'pop')::float                  as pop,
        (day_record ->> 'uvi')::float                  as uvi,
        (day_record ->> 'rain')::float                 as rain,
        (day_record ->> 'clouds')::int                 as clouds,
        to_timestamp((day_record ->> 'sunrise')::bigint)             as sunrise,
        to_timestamp((day_record ->> 'sunset')::bigint)              as sunset,
        (day_record ->> 'humidity')::int               as humidity,
        (day_record ->> 'pressure')::int               as pressure,
        (day_record ->> 'wind_deg')::int               as wind_deg,
        (day_record ->> 'dew_point')::float            as dew_point,
        (day_record ->> 'wind_gust')::float            as wind_gust,
        -- temp nested object
        (day_record -> 'temp' ->> 'day')::float       as temp_day,
        (day_record -> 'temp' ->> 'min')::float       as temp_min,
        (day_record -> 'temp' ->> 'max')::float       as temp_max,
        (day_record -> 'temp' ->> 'night')::float     as temp_night,
        (day_record -> 'temp' ->> 'eve')::float       as temp_eve,
        (day_record -> 'temp' ->> 'morn')::float      as temp_morn,
        -- feels_like nested object
        (day_record -> 'feels_like' ->> 'day')::float   as feels_like_day,
        (day_record -> 'feels_like' ->> 'night')::float as feels_like_night,
        (day_record -> 'feels_like' ->> 'eve')::float   as feels_like_eve,
        (day_record -> 'feels_like' ->> 'morn')::float  as feels_like_morn,
        -- weather array first element
        (day_record -> 'weather' -> 0 ->> 'main')       as weather_main,
        (day_record -> 'weather' -> 0 ->> 'description') as weather_description,
        day_record ->> 'summary'                        as summary,
        to_timestamp((day_record ->> 'moonrise')::bigint)            as moonrise,
        to_timestamp((day_record ->> 'moonset')::bigint )            as moonset,
        -- synthetic unique key
        concat((day_record ->> 'dt')::text, '_', lat::text, '_', lon::text) as dt_lat_lon
    from exploded
)

select * from parsed


{% if is_incremental() %}
where dt_lat_lon not in (select dt_lat_lon from {{ this }})
{% endif %}