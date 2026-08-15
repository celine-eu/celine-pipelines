-- Percentages, bearings and magnitudes must be physically possible.
--
-- The facade normalises several providers into one schema, and unit mistakes
-- are the failure mode that survives every other check: a provider reporting
-- wind in km/h into a `_ms` column, cloud cover as a 0-1 fraction into a `_pct`
-- column, or a bearing in radians, all produce well-typed rows that pass
-- not_null and uniqueness and are wrong by an order of magnitude. Consumers
-- render them without question.
--
-- NULL is not a violation — a provider that does not report a variable leaves
-- it null, and `not between` yields NULL for those rows, which the WHERE clause
-- drops.

with rows_under_test as (

    select 'weather_forecast_hourly' as model, location_id, provider,
           forecast_at::text as slot,
           humidity_pct, cloud_cover_pct, precipitation_probability_pct,
           wind_direction_deg, wind_speed_ms, wind_gust_ms, precipitation_mm
    from {{ ref('weather_forecast_hourly') }}

    union all

    select 'weather_forecast_daily', location_id, provider,
           forecast_date::text,
           null::double precision, cloud_cover_pct, precipitation_probability_pct,
           wind_direction_deg, wind_speed_ms, wind_gust_ms, precipitation_mm
    from {{ ref('weather_forecast_daily') }}

    union all

    select 'weather_current', location_id, provider,
           observed_at::text,
           humidity_pct, cloud_cover_pct, null::integer,
           wind_direction_deg, wind_speed_ms, wind_gust_ms, precipitation_mm
    from {{ ref('weather_current') }}

)

select *
from rows_under_test
where humidity_pct                    not between 0 and 100
   or cloud_cover_pct                 not between 0 and 100
   or precipitation_probability_pct   not between 0 and 100
   or wind_direction_deg              not between 0 and 360
   or wind_speed_ms                   < 0
   or wind_gust_ms                    < 0
   or precipitation_mm                < 0
