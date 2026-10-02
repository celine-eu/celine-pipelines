{{ config(
    materialized = 'view'
) }}

{#
    Staging view for Open-Meteo soil data.
    Type-casts raw columns and computes soil_1m_c: a depth-weighted
    composite at 100cm (1m) depth, linearly interpolated between the two
    ERA5-Land layer midpoints (64cm for 28-100cm, 177.5cm for 100-255cm).
    Weights mirrored in flows/pipeline_soil.py (_soil_1m) -- keep the two
    in sync if these ever change. soil_1m_c is NULL whenever either source
    layer is NULL; the not-null filter lives in the silver layer.
    No deduplication here -- done in silver layer.
#}

with source as (

    select
        cast(date as date)                                 as date,
        cast(lat as double precision)                      as lat,
        cast(lon as double precision)                      as lon,
        cast(soil_temperature_28_to_100cm_mean as float)   as soil_28_100cm_c,
        cast(soil_temperature_100_to_255cm_mean as float)  as soil_100_255cm_c,
        cast(api_lat as double precision)                  as api_lat,
        cast(api_lon as double precision)                  as api_lon,
        _sdc_extracted_at
    from {{ source('raw', 'om_weather_soil') }}

),

with_composite as (

    select
        *,
        0.683 * soil_28_100cm_c + 0.317 * soil_100_255cm_c as soil_1m_c
    from source

)

select * from with_composite
