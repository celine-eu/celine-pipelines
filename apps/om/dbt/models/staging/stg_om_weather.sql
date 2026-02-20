{{ config(
    materialized = 'incremental',
    unique_key   = ['datetime'],
    incremental_strategy = 'merge'
) }}

with source as (

    select
        cast(datetime as timestamp)                    as datetime,
        cast(shortwave_radiation as float)             as shortwave_radiation,
        cast(direct_radiation as float)                as direct_radiation,
        cast(diffuse_radiation as float)               as diffuse_radiation,
        cast(global_tilted_irradiance as float)        as global_tilted_irradiance,
        cast(cloud_cover as float)                     as cloud_cover,
        cast(temperature_2m as float)                  as temperature_2m,
        cast(precipitation as float)                   as precipitation,
        _sdc_extracted_at
    from {{ source('raw', 'om_weather') }}

    {% if is_incremental() %}
    where _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ this }})
    {% endif %}

)

select * from source
