{{ config(materialized='view') }}

{# Same naive Europe/Rome times as stg_om_weather_current. #}

select
    location_name,
    cast(latitude as float)                            as lat,
    cast(longitude as float)                           as lon,
    cast(date as date)                                 as forecast_date,
    cast(uv_index_max as float)                        as uv_index_max,
    cast(sunrise as timestamp) at time zone 'Europe/Rome' as sunrise,
    cast(sunset as timestamp) at time zone 'Europe/Rome'  as sunset,
    _sdc_extracted_at                                  as extracted_at
from {{ source('raw', 'om_weather_daily') }}
