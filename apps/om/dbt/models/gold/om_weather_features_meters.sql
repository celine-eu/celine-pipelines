{{ config(
    materialized = 'incremental',
    unique_key   = ['datetime'],
    incremental_strategy = 'merge'
) }}

{#
    Gold-layer features for PV/solar metering.
    15 features focused on solar production forecasting.
    Identical output for historical and forecast data (train/test parity).
#}

with source as (

    select
        cast(datetime as timestamp)                          as datetime,

        -- Temporal (5)
        cast(hour_sin as float)                              as hour_sin,
        cast(hour_cos as float)                              as hour_cos,
        cast(day_of_week as int)                             as day_of_week,
        cast(month as int)                                   as month,
        cast(is_weekend as int)                              as is_weekend,

        -- Weather pass-through (4)
        cast(global_tilted_irradiance as float)              as global_tilted_irradiance,
        cast(shortwave_radiation as float)                   as shortwave_radiation,
        cast(cloud_cover as float)                           as cloud_cover,
        cast(temperature_2m as float)                        as temperature_2m,

        -- Solar-derived (3)
        cast(clearsky_index as float)                        as clearsky_index,
        cast(effective_solar_pv as float)                    as effective_solar_pv,
        cast(theoretical_prod as float)                      as theoretical_prod,

        -- Temperature-derived (2)
        cast(heating_degree as float)                        as heating_degree,
        cast(cooling_degree as float)                        as cooling_degree,

        -- Daylight (1)
        cast(is_daylight as int)                             as is_daylight,

        _sdc_extracted_at

    from {{ source('raw', 'om_weather_features_meters') }}

    {% if is_incremental() %}
    where _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ this }})
    {% endif %}

)

select * from source
