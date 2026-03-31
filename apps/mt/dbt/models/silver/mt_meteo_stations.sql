{{ config(materialized='table') }}

select
    code,
    name,
    shortname,
    cast(elevation  as integer) as elevation_m,
    cast(latitude   as float)   as latitude,
    cast(longitude  as float)   as longitude,
    cast(east       as float)   as east,
    cast(north      as float)   as north,
    cast(startdate  as date)    as active_since,
    cast(enddate    as date)    as active_until
from {{ ref('stg_mt_meteo_stations') }}
