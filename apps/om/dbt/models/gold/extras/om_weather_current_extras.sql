{{ config(materialized='table') }}

select
    location_name,
    lat,
    lon,
    observed_at,
    humidity_pct,
    uv_index,
    cloud_cover_pct
from (
    select
        *,
        row_number() over (
            partition by location_name
            order by observed_at desc
        ) as rn
    from {{ ref('stg_om_weather_current') }}
) latest
where rn = 1
