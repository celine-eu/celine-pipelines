-- Percentages, bearings and magnitudes must be physically possible.
--
-- This layer does nothing but cast: every column is `cast(x as float)` off a
-- raw payload. A cast is exactly the operation that turns a unit change, a
-- decimal-comma locale, or a renamed upstream field into a well-typed wrong
-- number. Nothing else in the pipeline would notice — the values flow straight
-- into the weather contract tables and out to digital-twin and celine-webapp.
--
-- NULL is not a violation: MeteoTrentino omits variables a station does not
-- measure, and `not between` yields NULL for those, which the WHERE drops.

with rows_under_test as (

    select
        'mt_station_observations' as model,
        station_code::text        as entity,
        observed_at::text         as slot,
        relative_humidity_pct     as pct,
        wind_direction_deg        as bearing,
        wind_speed_ms,
        wind_gust_ms,
        precipitation_mm,
        snow_depth_cm             as snow,
        air_temperature_c         as temperature_c,
        global_radiation_wm2      as radiation
    from {{ ref('mt_station_observations') }}

    union all

    select
        'mt_forecasts_hourly',
        location_id::text,
        forecast_at::text,
        rain_probability_pct,
        wind_direction_deg,
        wind_speed_ms,
        wind_gust_ms,
        rain_fall_mm,
        fresh_snow_cm,
        temperature_c,
        null::float
    from {{ ref('mt_forecasts_hourly') }}

    union all

    select
        'mt_forecasts_daily',
        location_id::text,
        forecast_at::text,
        rain_probability_pct,
        wind_direction_deg,
        wind_speed_ms,
        wind_gust_ms,
        rain_fall_mm,
        fresh_snow_cm,
        temperature_c,
        null::float
    from {{ ref('mt_forecasts_daily') }}

)

select *
from rows_under_test
where pct              not between 0 and 100
   or bearing          not between 0 and 360
   or wind_speed_ms    < 0
   or wind_gust_ms     < 0
   or precipitation_mm < 0
   or snow             < 0
   or radiation        < 0
   -- Alpine range with a wide margin. This catches a Kelvin or Fahrenheit
   -- upstream, not a cold morning: the observed extremes across the station
   -- network sit far inside it.
   or temperature_c    not between -60 and 60
