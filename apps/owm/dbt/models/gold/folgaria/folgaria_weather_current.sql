with source as (
    select *
    from {{ ref('weather_current') }}
    where lower(location_id) like '%folgaria%'
),

dedup as (
    select *,
           row_number() over (
               partition by dt_lat_lon
               order by synced_at desc
           ) as rn
    from source
)

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
from dedup
where rn = 1