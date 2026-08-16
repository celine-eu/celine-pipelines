{{ config(
    materialized         = 'incremental',
    unique_key           = ['location_id', 'forecast_at'],
    incremental_strategy = 'merge',
    on_schema_change     = 'append_new_columns'
) }}

{#
    Sub-daily forecasts per configured location, resolved to the nearest
    provider location.

    Ranking is lexicographic, not a combined metric:

      1. horizontal distance   — the primary criterion, unchanged
      2. elevation difference  — breaks ties between candidates at one coordinate
      3. location_id           — breaks everything else, so the result is stable

    Step 2 exists because a coordinate is not a unique forecast point. Providers
    publish mountain massifs as a vertical profile: one representative lat/lon
    and a separate location per altitude band. Those tie at horizontal distance
    zero, and before this the winner was whichever row Postgres returned first —
    a swing of ~10 °C between runs with no change in the data.

    Kept lexicographic on purpose. Folding elevation into the distance would need
    a metres-per-degree exchange rate that nothing justifies, and would change
    which row wins in cases that were never ambiguous. This ordering is identical
    to the previous one wherever the horizontal distance already had a unique
    minimum, so it can only change results where the old one was arbitrary.

    `elevation_m` is optional on both sides. NULLS LAST means a candidate that
    declares its elevation beats one that does not; if neither does, step 3
    still decides. A seed row with no elevation therefore keeps the old
    behaviour minus the non-determinism.
#}

with locations as (
    select * from {{ ref('weather_locations') }}
),

ranked as (
    select
        loc.location_id,
        src.provider,
        src.forecast_at,
        src.elevation_m,
        src.temperature_c,
        src.humidity_pct,
        src.wind_speed_ms,
        src.wind_gust_ms,
        src.wind_direction_deg,
        src.precipitation_mm,
        src.precipitation_probability_pct,
        src.cloud_cover_pct,
        src.sky_condition,
        src.pressure_hpa,
        src.snow_cm,
        row_number() over (
            partition by loc.location_id, src.forecast_at
            order by
                (abs(src.lat - loc.lat) + abs(src.lon - loc.lon)),
                abs(src.elevation_m - loc.elevation_m) nulls last,
                src.location_id
        ) as rn
    from {{ ref('stg_weather_forecast_hourly') }} src
    join locations loc
        on abs(src.lat - loc.lat) < 0.1
        and abs(src.lon - loc.lon) < 0.1
)

select
    location_id,
    provider,
    forecast_at,
    elevation_m,
    temperature_c,
    humidity_pct,
    wind_speed_ms,
    wind_gust_ms,
    wind_direction_deg,
    precipitation_mm,
    precipitation_probability_pct,
    cloud_cover_pct,
    sky_condition,
    pressure_hpa,
    snow_cm
from ranked
where rn = 1
