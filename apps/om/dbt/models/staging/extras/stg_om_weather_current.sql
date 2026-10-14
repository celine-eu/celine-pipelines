{{ config(materialized='view') }}

{#
    tap-openmeteo asks for timezone=Europe/Rome and gets local times back with
    no offset; target-postgres stores them as naive timestamps. Anchoring them
    here is what lets every model downstream speak UTC.
#}

select
    location_name,
    cast(latitude as float)                         as lat,
    cast(longitude as float)                        as lon,
    cast(time as timestamp) at time zone 'Europe/Rome' as observed_at,
    cast(relative_humidity_2m as float)             as humidity_pct,
    cast(uv_index as float)                         as uv_index,
    cast(cloud_cover as float)                      as cloud_cover_pct
from {{ source('raw', 'om_weather_current') }}
