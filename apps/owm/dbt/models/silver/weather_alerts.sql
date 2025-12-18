{{ config(
    materialized='incremental',
    schema='silver',
    unique_key='dt_lat_lon',
    incremental_strategy='merge',
    alias='weather_alerts',
    description="Weather alert data from OpenWeather API, normalized and stored incrementally for emergency preparedness"
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
        a as alert_record
    from source s,
         unnest(coalesce(s.alerts, '{}')) as a  -- handle empty arrays
),

parsed as (
    select
        synced_at,
        lat,
        lon,
        location_id,
        alert_record ->> 'sender_name'   as sender_name,
        alert_record ->> 'event'         as event,
        to_timestamp((alert_record ->> 'start')::bigint) as start_ts,
        to_timestamp((alert_record ->> 'end')::bigint) as end_ts,
        alert_record ->> 'description'   as description,
        -- synthetic unique key
        concat((alert_record ->> 'start')::text, '_', (alert_record ->> 'end')::text, '_', lat::text, '_', lon::text) as dt_lat_lon
    from exploded
)

select * from parsed


{% if is_incremental() %}
where dt_lat_lon not in (select dt_lat_lon from {{ this }})
{% endif %}