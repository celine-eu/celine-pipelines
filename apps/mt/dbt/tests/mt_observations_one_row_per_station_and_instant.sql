-- Composite keys must actually be unique.
--
-- All three models declare a two-column `unique_key` and merge on it. On
-- Postgres that deduplicates the *incoming* batch against the target; it does
-- not stop the target disagreeing with itself, and no database constraint backs
-- it up. A source that starts emitting two rows for one (station, instant) —
-- which happens when an upstream re-publishes a corrected observation under a
-- new ingestion timestamp — lands both, and every consumer silently doubles
-- whatever it aggregates.
--
-- Expressed as a singular test because this app installs no dbt packages;
-- dbt_utils.unique_combination_of_columns would mean a package dependency in
-- the image for a two-column check.

select 'mt_station_observations' as model, station_code::text as key_a, observed_at::text as key_b, count(*) as rows
from {{ ref('mt_station_observations') }}
group by station_code, observed_at
having count(*) > 1

union all

select 'mt_forecasts_hourly', location_id::text, forecast_at::text, count(*)
from {{ ref('mt_forecasts_hourly') }}
group by location_id, forecast_at
having count(*) > 1

union all

select 'mt_forecasts_daily', location_id::text, forecast_at::text, count(*)
from {{ ref('mt_forecasts_daily') }}
group by location_id, forecast_at
having count(*) > 1
