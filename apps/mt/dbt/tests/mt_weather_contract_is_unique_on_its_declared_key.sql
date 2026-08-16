-- The weather contract tables must be unique on the key they declare.
--
-- These four are read by `apps/weather`, which is read by digital-twin and
-- celine-webapp. Each declares a `unique_key` and merges on it, which on
-- Postgres deduplicates the incoming batch against the target and nothing more.
-- A duplicate here fans out through the facade into every consumer.
--
-- The declared key is (location_id, forecast_at) — deliberately NOT
-- (lat, lon, forecast_at). MeteoTrentino publishes several forecast points at
-- one coordinate, so coordinates are not a key at this layer; see
-- mt_weather_contract_elevations_collide_on_one_coordinate.sql.

select 'weather__forecast_hourly' as contract, location_id, forecast_at::text as slot, count(*) as rows
from {{ ref('mt_weather_forecast_hourly') }}
group by location_id, forecast_at
having count(*) > 1

union all

select 'weather__forecast_daily', location_id, forecast_date::text, count(*)
from {{ ref('mt_weather_forecast_daily') }}
group by location_id, forecast_date
having count(*) > 1

union all

select 'weather__current', station_id, observed_at::text, count(*)
from {{ ref('mt_weather_current') }}
group by station_id, observed_at
having count(*) > 1
