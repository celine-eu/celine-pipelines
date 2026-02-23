{{ config(
    materialized = 'incremental',
    unique_key   = ['datetime'],
    incremental_strategy = 'merge'
) }}

{#
    Hourly weather data for energy forecasting.
    Outputs the 4 natural weather variables only, identical for
    both historical and forecast sources.
#}

with base as (

    select *
    from {{ ref('stg_om_weather') }}
    {% if is_incremental() %}
    where datetime > (
        select coalesce(max(datetime), '1900-01-01'::timestamp)
        from {{ this }}
    )
    {% endif %}

)

select
    datetime,
    shortwave_radiation,
    direct_radiation,
    diffuse_radiation,
    global_tilted_irradiance,
    cloud_cover,
    temperature_2m,
    precipitation
from base
