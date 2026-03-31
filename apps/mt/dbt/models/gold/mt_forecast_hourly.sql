{{ config(materialized='table') }}

{#
    Current and future sub-daily (3-hour interval) forecasts enriched with
    location metadata. Filters to forecast_at >= now() - 1h to exclude stale
    past intervals while retaining the most recent completed period.
#}

select
    f.location_id,
    l.name_it            as location_name_it,
    l.name_en            as location_name_en,
    l.elevation_m        as location_elevation_m,
    l.latitude,
    l.longitude,
    f.forecast_at,
    f.interval_minutes,
    f.temperature_c,
    f.rain_fall_mm,
    f.fresh_snow_cm,
    f.snow_level_m,
    f.wind_speed_ms,
    f.wind_gust_ms,
    f.wind_direction_deg,
    f.sky_condition,
    f.freezing_level_m,
    f.rain_probability_pct,
    f.sunshine_duration_min
from {{ ref('mt_forecasts_hourly') }} f
join {{ ref('mt_forecast_locations') }} l on l.location_id = f.location_id
where f.forecast_at >= now() - interval '1 hour'
