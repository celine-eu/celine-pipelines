{{ config(materialized='table') }}

select
    id                          as location_id,
    name_it,
    name_de,
    name_en,
    name_lld,
    cast(elevation  as integer) as elevation_m,
    cast(latitude   as float)   as latitude,
    cast(longitude  as float)   as longitude,
    cast(venue_type as integer) as venue_type,
    forecast_url
from {{ ref('stg_mt_forecast_locations') }}
