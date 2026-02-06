{{ config(
    materialized = 'incremental',
    unique_key   = ['datetime'],
    incremental_strategy = 'merge'
) }}

with source as (

    select
        cast(datetime as timestamp)                    as datetime,
        cast(direct_radiation as float)                as direct_radiation,
        cast(diffuse_radiation as float)               as diffuse_radiation,
        cast(direct_normal_irradiance as float)        as direct_normal_irradiance,
        cast(shortwave_radiation as float)             as shortwave_radiation,
        cast(cloud_cover as float)                     as cloud_cover,
        cast(cloud_cover_low as float)                 as cloud_cover_low,
        cast(cloud_cover_mid as float)                 as cloud_cover_mid,
        cast(cloud_cover_high as float)                as cloud_cover_high,
        cast(temperature_2m as float)                  as temperature_2m,
        cast(apparent_temperature as float)            as apparent_temperature,
        cast(precipitation as float)                   as precipitation,
        cast(rain as float)                            as rain,
        cast(snowfall as float)                        as snowfall,
        cast(relative_humidity_2m as float)            as relative_humidity_2m,
        _sdc_extracted_at
    from {{ source('raw', 'om_weather') }}

    {% if is_incremental() %}
    where _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ this }})
    {% endif %}

)

select * from source
