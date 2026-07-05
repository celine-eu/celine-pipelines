{{ config(
    materialized         = 'incremental',
    unique_key           = ['location_id', 'forecast_date'],
    incremental_strategy = 'merge',
    alias                = 'weather__forecast_daily'
) }}

{#
    Weather contract: daily forecast summaries normalized to the shared schema
    consumed by the weather pipeline. One row per (location_id, forecast_date).

    sky_condition is resolved to name_eng via mt_sky_conditions lookup.
#}

select
    'mt'::text                          as provider,
    f.location_id::text                 as location_id,
    l.name_en                           as location_name,
    l.latitude                          as lat,
    l.longitude                         as lon,
    f.forecast_at::date                 as forecast_date,
    f.temperature_c,
    f.temperature_min_c,
    f.temperature_max_c,
    f.rain_fall_mm                      as precipitation_mm,
    f.rain_probability_pct              as precipitation_probability_pct,
    f.wind_speed_ms,
    f.wind_gust_ms,
    f.wind_direction_deg::int           as wind_direction_deg,
    null::float                         as cloud_cover_pct,
    coalesce(sc.name_eng, f.sky_condition) as sky_condition,
    f.fresh_snow_cm                     as snow_cm,
    null::timestamptz                   as sunrise,
    null::timestamptz                   as sunset
from {{ ref('mt_forecasts_daily') }} f
join {{ ref('mt_forecast_locations') }} l on l.location_id = f.location_id
left join {{ ref('mt_sky_conditions') }} sc on lower(sc.id) = lower(f.sky_condition)
where f.forecast_at >= now() - interval '1 hour'
