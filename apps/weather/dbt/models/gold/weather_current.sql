{{ config(materialized='table') }}

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
        src.station_id,
        src.station_name,
        src.observed_at,
        src.elevation_m,
        src.temperature_c,
        src.humidity_pct,
        src.wind_speed_ms,
        src.wind_direction_deg,
        src.wind_gust_ms,
        src.precipitation_mm,
        src.cloud_cover_pct,
        src.sky_condition,
        src.pressure_hpa,
        row_number() over (
            partition by loc.location_id
            order by
                (abs(src.lat - loc.lat) + abs(src.lon - loc.lon)),
                abs(src.elevation_m - loc.elevation_m) nulls last,
                src.station_id
        ) as rn
    from {{ ref('stg_weather_current') }} src
    cross join locations loc
    where abs(src.lat - loc.lat) < 0.15
      and abs(src.lon - loc.lon) < 0.15
),

{#
    Open-Meteo extras fill what the station does not report — humidity, cloud
    cover — and add what no station reports: UV, sunrise, sunset. The station's
    own value always wins; the extras are fetched for a few points only, hence
    the same 0.1-degree radius as the forecasts so a distant location gets
    nothing rather than someone else's sky.
#}

current_extras as (
    select
        loc.location_id,
        ex.humidity_pct,
        ex.uv_index,
        ex.cloud_cover_pct,
        row_number() over (
            partition by loc.location_id
            order by
                (abs(ex.lat - loc.lat) + abs(ex.lon - loc.lon)),
                ex.location_name
        ) as rn
    from {{ ref('stg_om_weather_current_extras') }} ex
    join locations loc
        on abs(ex.lat - loc.lat) < 0.1
        and abs(ex.lon - loc.lon) < 0.1
),

daily_extras as (
    select
        loc.location_id,
        ex.forecast_date,
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
    r.station_id,
    r.station_name,
    r.observed_at,
    r.elevation_m,
    r.temperature_c,
    coalesce(r.humidity_pct, ce.humidity_pct)       as humidity_pct,
    r.wind_speed_ms,
    r.wind_direction_deg,
    r.wind_gust_ms,
    r.precipitation_mm,
    coalesce(r.cloud_cover_pct, ce.cloud_cover_pct) as cloud_cover_pct,
    r.sky_condition,
    r.pressure_hpa,
    ce.uv_index,
    de.sunrise,
    de.sunset
from ranked r
left join current_extras ce
    on ce.location_id = r.location_id
    and ce.rn = 1
-- The extras are Trentino points, so their local day is Europe/Rome's.
left join daily_extras de
    on de.location_id = r.location_id
    and de.rn = 1
    and de.forecast_date = cast(r.observed_at at time zone 'Europe/Rome' as date)
where r.rn = 1
