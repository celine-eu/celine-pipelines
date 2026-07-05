{{ config(
    materialized         = 'incremental',
    unique_key           = ['location_id', 'forecast_at'],
    incremental_strategy = 'merge',
    alias                = 'weather__forecast_hourly'
) }}

{#
    Weather contract: sub-daily forecasts normalized to the shared schema
    consumed by the weather pipeline. One row per (location_id, forecast_at).

    sky_condition is resolved to name_eng via mt_sky_conditions lookup.
#}

select
    'mt'::text                          as provider,
    f.location_id::text                 as location_id,
    l.name_en                           as location_name,
    l.latitude                          as lat,
    l.longitude                         as lon,
    f.forecast_at,
    f.temperature_c,
    null::float                         as humidity_pct,
    f.wind_speed_ms,
    f.wind_gust_ms,
    f.wind_direction_deg::int           as wind_direction_deg,
    f.rain_fall_mm                      as precipitation_mm,
    f.rain_probability_pct              as precipitation_probability_pct,
    null::float                         as cloud_cover_pct,
    coalesce(sc.name_eng, f.sky_condition) as sky_condition,
    null::float                         as pressure_hpa,
    f.fresh_snow_cm                     as snow_cm
from {{ ref('mt_forecasts_hourly') }} f
join {{ ref('mt_forecast_locations') }} l on l.location_id = f.location_id
left join {{ ref('mt_sky_conditions') }} sc on lower(sc.id) = lower(f.sky_condition)
where f.forecast_at >= now() - interval '1 hour'
