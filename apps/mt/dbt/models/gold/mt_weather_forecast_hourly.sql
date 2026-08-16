{{ config(
    materialized         = 'incremental',
    unique_key           = ['location_id', 'forecast_at'],
    incremental_strategy = 'merge',
    on_schema_change     = 'append_new_columns',
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
    -- Part of the contract, not decoration. MeteoTrentino publishes mountain
    -- massifs as a vertical profile: one representative lat/lon per massif and
    -- a separate location per altitude band (1500/2000/2500/3000 m). Without
    -- this column those bands are indistinguishable to the weather facade,
    -- which matches by coordinate — it would tie at distance zero and pick one
    -- at random, a ~10 °C swing between runs. See
    -- .agents/knowledge/mt-shares-coordinates-across-locations.md
    l.elevation_m,
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
