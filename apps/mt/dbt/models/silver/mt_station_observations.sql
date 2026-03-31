{{ config(
    materialized         = 'incremental',
    unique_key           = ['station_code', 'observed_at'],
    incremental_strategy = 'merge'
) }}

select
    station_code,
    (cast(timestamp as timestamp) at time zone 'Europe/Rome') as observed_at,
    cast(air_temperature_c     as float)       as air_temperature_c,
    cast(precipitation_mm      as float)       as precipitation_mm,
    cast(wind_speed_ms         as float)       as wind_speed_ms,
    cast(wind_direction_deg    as float)       as wind_direction_deg,
    cast(wind_gust_ms          as float)       as wind_gust_ms,
    cast(global_radiation_wm2  as float)       as global_radiation_wm2,
    cast(relative_humidity_pct as float)       as relative_humidity_pct,
    cast(snow_depth_cm         as float)       as snow_depth_cm
from {{ ref('stg_mt_station_observations') }}

{% if is_incremental() %}
where (cast(timestamp as timestamp) at time zone 'Europe/Rome') > (
    select coalesce(max(observed_at), '1900-01-01'::timestamptz) - interval '7 days'
    from {{ this }}
)
{% endif %}
