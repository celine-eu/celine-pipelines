{{ config(
    materialized         = 'incremental',
    unique_key           = ['location_id', 'forecast_at'],
    incremental_strategy = 'merge',
    on_schema_change     = 'append_new_columns'
) }}

{#
    Raw holds both timestamp forms (see mt_forecast_instant) inside the staging
    window, so one instant can arrive twice: keep the most recent extraction.

    No incremental filter on the forecast instant: the staging view already bounds
    by extraction time, and bounding by instant froze each forecast at the first
    extraction that reached it, 3-6 days ahead.
#}

select distinct on (location_id, forecast_at) *
from (
    select
        location_id,
        {{ mt_forecast_instant('forecast_timestamp', '_sdc_extracted_at') }} as forecast_at,
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
        cast(sunshine_duration   as float)       as sunshine_duration_min,
        _sdc_extracted_at                        as extracted_at
    from {{ ref('stg_mt_forecasts_hourly') }}
) f
order by location_id, forecast_at, extracted_at desc nulls last
