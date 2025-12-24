{{ config(
    materialized='view'
) }}

with source as (
    select * 
    from {{ source('radiation', 'cams_solar_radiation_timeseries') }}
)

select
    -- optional TOA (not always provided by CAMS)
    {% if 'toa' in adapter.get_columns_in_relation(
         source('radiation','cams_solar_radiation_timeseries')
       ) | map(attribute='name') | list %}
      nullif(toa, 'nan')::float as toa,
    {% else %}
      null::float as toa,
    {% endif %}


    nullif(trim(clear_sky_ghi), '')::float as clear_sky_ghi,
    nullif(trim(clear_sky_bhi), '')::float as clear_sky_bhi,
    nullif(trim(clear_sky_dhi), '')::float as clear_sky_dhi,
    nullif(trim(clear_sky_bni), '')::float as clear_sky_bni,

    nullif(trim(ghi), '')::float as ghi,
    nullif(trim(bhi), '')::float as bhi,
    nullif(trim(dhi), '')::float as dhi,
    nullif(trim(bni), '')::float as bni,

    nullif(trim(reliability), '')::float as reliability,
    date_start
from source