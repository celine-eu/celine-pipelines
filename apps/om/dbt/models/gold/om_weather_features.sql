{{ config(
    materialized = 'incremental',
    unique_key   = ['datetime'],
    incremental_strategy = 'merge'
) }}

{#
    Gold-layer ML features for energy consumption forecasting.
    29 features computed from the 4 natural weather variables + datetime.
    Identical output for historical and forecast data (train/test parity).
#}

with source as (

    select
        cast(datetime as timestamp)                          as datetime,

        -- Temporal / Fourier (11)
        cast(hour_sin as float)                              as hour_sin,
        cast(hour_cos as float)                              as hour_cos,
        cast(dow_sin as float)                               as dow_sin,
        cast(dow_cos as float)                               as dow_cos,
        cast(annual_sin as float)                            as annual_sin,
        cast(annual_cos as float)                            as annual_cos,
        cast(semi_annual_sin as float)                       as semi_annual_sin,
        cast(semi_annual_cos as float)                       as semi_annual_cos,
        cast(is_weekend as int)                              as is_weekend,
        cast(is_holiday as int)                              as is_holiday,
        cast(is_daylight as int)                             as is_daylight,

        -- Temperature-derived (11)
        cast(temperature_2m as float)                        as temperature_2m,
        cast(heating_degree_hour as float)                   as heating_degree_hour,
        cast(temp_rolling_mean_24h as float)                 as temp_rolling_mean_24h,
        cast(temp_rolling_std_24h as float)                  as temp_rolling_std_24h,
        cast(temp_change_rate_3h as float)                   as temp_change_rate_3h,
        cast(thermal_inertia_12h as float)                   as thermal_inertia_12h,
        cast(temp_gradient_24h as float)                     as temp_gradient_24h,
        cast(heating_degree_rolling_mean_24h as float)       as heating_degree_rolling_mean_24h,
        cast(cumulative_hdd_48h as float)                    as cumulative_hdd_48h,
        cast(temp_x_hour_sin as float)                       as temp_x_hour_sin,
        cast(heating_x_night as float)                       as heating_x_night,

        -- Radiation-derived (3)
        cast(shortwave_radiation as float)                   as shortwave_radiation,
        cast(radiation_rolling_mean_24h as float)            as radiation_rolling_mean_24h,
        cast(radiation_x_daytime as float)                   as radiation_x_daytime,

        -- Cloud-derived (2)
        cast(cloud_cover as float)                           as cloud_cover,
        cast(cloud_cover_rolling_mean_24h as float)          as cloud_cover_rolling_mean_24h,

        -- Precipitation (1)
        cast(precipitation as float)                         as precipitation,

        -- Interaction (1)
        cast(weekend_x_hour_cos as float)                    as weekend_x_hour_cos,

        _sdc_extracted_at

    from {{ source('raw', 'om_weather_features') }}

    {% if is_incremental() %}
    where _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ this }})
    {% endif %}

)

select * from source
