{{ config(
    materialized         = 'incremental',
    unique_key           = ['location_id', 'forecast_at'],
    incremental_strategy = 'merge'
) }}

select
    location_id,
    (cast(forecast_timestamp as timestamp) at time zone 'Europe/Rome') as forecast_at,
    cast(interval_minutes    as integer)     as interval_minutes,
    cast(temperature         as float)       as temperature_c,
    cast(rain_fall           as float)       as rain_fall_mm,
    cast(fresh_snow          as float)       as fresh_snow_cm,
    cast(snow_level          as integer)     as snow_level_m,
    cast(wind_speed          as float)       as wind_speed_ms,
    cast(wind_gust           as float)       as wind_gust_ms,
    cast(wind_direction      as integer)     as wind_direction_deg,
    sky_condition,
    cast(freezing_level      as integer)     as freezing_level_m,
    cast(rain_probability    as integer)     as rain_probability_pct,
    cast(sunshine_duration   as float)       as sunshine_duration_min
from {{ ref('stg_mt_forecasts_hourly') }}

{% if is_incremental() %}
where (cast(forecast_timestamp as timestamp) at time zone 'Europe/Rome') > (
    select coalesce(max(forecast_at), '1900-01-01'::timestamptz) - interval '3 days'
    from {{ this }}
)
{% endif %}
