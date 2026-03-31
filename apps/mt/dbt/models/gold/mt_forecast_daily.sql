{{ config(materialized='table') }}

{#
    Current and future daily forecast summaries enriched with location metadata.
    Filters to forecast_at >= now() - 1h to exclude yesterday's summary
    while retaining today's day if it started within the last hour.
#}

select
    f.location_id,
    l.name_it            as location_name_it,
    l.name_en            as location_name_en,
    l.elevation_m        as location_elevation_m,
    l.latitude,
    l.longitude,
    f.forecast_at,
    f.temperature_c,
    f.temperature_max_c,
    f.temperature_min_c,
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
from {{ ref('mt_forecasts_daily') }} f
join {{ ref('mt_forecast_locations') }} l on l.location_id = f.location_id
where f.forecast_at >= now() - interval '1 hour'
