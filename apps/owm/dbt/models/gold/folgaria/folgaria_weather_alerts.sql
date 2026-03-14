-- models/gold/folgaria/folgaria_weather_alerts.sql
{{ config(
    materialized='incremental',
    schema='gold',
    unique_key=['synced_at', 'event', 'start_ts'],
    incremental_strategy='merge',
    alias='folgaria_weather_alerts',
    description="Weather alerts for the Folgaria area"
) }}

with source as (
    select *
    from {{ ref('weather_alerts') }}
),

filtered as (
    select
        synced_at,
        lat,
        lon,
        location_id,
        sender_name,
        event,
        start_ts,
        end_ts,
        description,
        dt_lat_lon
    from source
    where lower(location_id) like '%folgaria%'
),

dedup as (
    select
        *,
        row_number() over (
            partition by synced_at, event, start_ts
            order by start_ts desc
        ) as rn
    from filtered
)

select
    synced_at,
    lat,
    lon,
    location_id,
    sender_name,
    event,
    start_ts,
    end_ts,
    description,
    dt_lat_lon
from dedup
where rn = 1