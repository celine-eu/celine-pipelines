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
    where _sdc_extracted_at > (
        select coalesce(max(_sdc_extracted_at), '1900-01-01'::timestamp)
        from {{ this }}
    )
    {% endif %}

)

select
    datetime,
    shortwave_radiation,
    cloud_cover,
    temperature_2m,
    precipitation
from base
