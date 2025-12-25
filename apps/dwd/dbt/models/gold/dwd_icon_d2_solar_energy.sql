{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['run_time_utc','interval_end_utc','lat','lon'],
    on_schema_change='sync_all_columns'
) }}

select
    'icon_d2'::text as provider,
    run_time_utc,
    interval_end_utc,
    lat,
    lon,

    ST_SetSRID(
        ST_MakePoint(lon, lat),
        4326
    )::geography as geog,

    -- energy over the accumulation period
    (value * interval_hours) / 1000.0
        as solar_energy_kwh_per_m2

from {{ ref('dwd_icon_d2_solar_forecast_interval') }}
