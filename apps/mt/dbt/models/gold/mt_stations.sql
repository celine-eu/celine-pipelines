{{ config(materialized='table') }}

{#
    Active station registry for spatial queries and lookups.
    Filters out decommissioned stations (enddate in the past).
#}

select
    code,
    name,
    shortname,
    elevation_m,
    latitude,
    longitude,
    active_since
from {{ ref('mt_meteo_stations') }}
where active_until is null or active_until >= current_date
