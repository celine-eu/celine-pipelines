{{ config(materialized='table') }}

{#
    target-postgres upserts on (location_name, date), so raw already holds one
    row per day; the ranking keeps that true if the key ever changes.
#}

select
    location_name,
    lat,
    lon,
    forecast_date,
    uv_index_max,
    sunrise,
    sunset
from (
    select
        *,
        row_number() over (
            partition by location_name, forecast_date
            order by extracted_at desc nulls last
        ) as rn
    from {{ ref('stg_om_weather_daily') }}
) latest
where rn = 1
