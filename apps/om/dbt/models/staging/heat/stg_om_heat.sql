{{ config(
    materialized = 'view'
) }}

{#
    Staging view for Open-Meteo heat data.
    Type-casts raw columns from daily temperature extraction.
    No deduplication here — done in silver layer.
#}

with source as (

    select
        cast(date as date)                    as date,
        cast(lat as double precision)         as lat,
        cast(lon as double precision)         as lon,
        cast(temperature_2m_max as float)     as temp_max_c,
        cast(elevation as float)              as elevation_m,
        model,
        _sdc_extracted_at
    from {{ source('raw', 'om_weather_heat') }}

)

select * from source
