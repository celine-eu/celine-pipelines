{{ config(
    materialized         = 'incremental',
    unique_key           = ['location_id', 'forecast_date'],
    incremental_strategy = 'merge',
    on_schema_change     = 'append_new_columns'
) }}

{#
    Ranking is lexicographic: horizontal distance first, then elevation
    difference to break ties between candidates sharing a coordinate, then
    location_id so the result is stable. See weather_forecast_hourly for the
    full reasoning — providers publish mountain massifs as a vertical profile at
    one representative lat/lon, and without the elevation term the winner among
    those bands was whichever row Postgres returned first.
#}

with locations as (
    select * from {{ ref('weather_locations') }}
),

ranked as (
    select
        loc.location_id,
        src.provider,
        src.forecast_date,
        src.elevation_m,
        src.temperature_c,
        src.temperature_min_c,
        src.temperature_max_c,
        src.precipitation_mm,
        src.precipitation_probability_pct,
        src.wind_speed_ms,
        src.wind_gust_ms,
        src.wind_direction_deg,
        src.cloud_cover_pct,
        src.sky_condition,
        src.snow_cm,
        src.sunrise,
        src.sunset,
        row_number() over (
            partition by loc.location_id, src.forecast_date
            order by
                (abs(src.lat - loc.lat) + abs(src.lon - loc.lon)),
                abs(src.elevation_m - loc.elevation_m) nulls last,
                src.location_id
        ) as rn
    from {{ ref('stg_weather_forecast_daily') }} src
    join locations loc
        on abs(src.lat - loc.lat) < 0.1
        and abs(src.lon - loc.lon) < 0.1
),

{#
    Open-Meteo's daily extras add the UV maximum, which no provider publishes,
    and fill sunrise and sunset where the provider leaves them null (mt does).
    Matched by date and by the same 0.1-degree radius as the provider rows.
#}

daily_extras as (
    select
        loc.location_id,
        ex.forecast_date,
        ex.uv_index_max,
        ex.sunrise,
        ex.sunset,
        row_number() over (
            partition by loc.location_id, ex.forecast_date
            order by
                (abs(ex.lat - loc.lat) + abs(ex.lon - loc.lon)),
                ex.location_name
        ) as rn
    from {{ ref('stg_om_weather_daily_extras') }} ex
    join locations loc
        on abs(ex.lat - loc.lat) < 0.1
        and abs(ex.lon - loc.lon) < 0.1
)

select
    r.location_id,
    r.provider,
    r.forecast_date,
    r.elevation_m,
    r.temperature_c,
    r.temperature_min_c,
    r.temperature_max_c,
    r.precipitation_mm,
    r.precipitation_probability_pct,
    r.wind_speed_ms,
    r.wind_gust_ms,
    r.wind_direction_deg,
    r.cloud_cover_pct,
    r.sky_condition,
    r.snow_cm,
    coalesce(r.sunrise, de.sunrise) as sunrise,
    coalesce(r.sunset, de.sunset)   as sunset,
    de.uv_index_max
from ranked r
left join daily_extras de
    on de.location_id = r.location_id
    and de.forecast_date = r.forecast_date
    and de.rn = 1
where r.rn = 1
