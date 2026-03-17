{{ config(
    materialized = 'view'
) }}

{#
    Staging view for Open-Meteo wind data.
    Type-casts raw columns and renames to standard names (m/s units).
    No deduplication here — done in silver layer.
#}

with source as (

    select
        cast(datetime as timestamp)          as datetime,
        cast(lat as double precision)        as lat,
        cast(lon as double precision)        as lon,
        cast(wind_speed_10m as float)        as wind_speed_ms,
        cast(wind_gusts_10m as float)        as wind_gusts_ms,
        cast(wind_direction_10m as float)    as wind_direction_deg,
        model,
        _sdc_extracted_at
    from {{ source('raw', 'om_weather_wind') }}

)

select * from source
