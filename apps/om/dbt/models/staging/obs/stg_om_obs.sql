{{ config(
    materialized = 'view'
) }}

{#
    Staging view for Open-Meteo 15-minute observations.
    Type-casts raw columns, renames to standard names with units,
    and converts naive Europe/Rome timestamps to proper TIMESTAMPTZ.
    No deduplication here — done in silver layer.
#}

with source as (

    select
        cast(datetime as timestamp)
            at time zone 'Europe/Rome'           as datetime,
        cast(lat as double precision)            as lat,
        cast(lon as double precision)            as lon,
        cast(temperature_2m as float)            as temp_c,
        cast(apparent_temperature as float)      as apparent_temp_c,
        cast(wind_speed_10m as float)            as wind_speed_ms,
        cast(wind_gusts_10m as float)            as wind_gusts_ms,
        cast(wind_direction_10m as float)        as wind_direction_deg,
        cast(precipitation as float)             as precip_mm,
        model,
        _sdc_extracted_at
    from {{ source('raw', 'om_observations') }}

)

select * from source
