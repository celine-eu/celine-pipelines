-- The facade must publish exactly one forecast per location per timestamp.
--
-- This is the whole point of the pipeline. Both forecast models rank provider
-- locations by distance and keep `rn = 1`, and both merge on a composite
-- unique_key. Neither mechanism is enforced by the database: `unique_key` on a
-- Postgres incremental merge deduplicates the *incoming* batch against the
-- target, not the target against itself, so two providers that both come within
-- the 0.1-degree radius of one seed location, or a seed file with two entries
-- at the same coordinates, yield two rows and no error. Consumers
-- (digital-twin, celine-webapp) read the first row they get and silently show
-- one provider's forecast on one refresh and another's on the next.
--
-- Expressed as a singular test rather than dbt_utils.unique_combination_of_columns
-- because this app installs no dbt packages; adding one for a two-column check
-- would put a package dependency into the image.

select 'weather_forecast_hourly' as model, location_id, forecast_at::text as slot, count(*) as rows
from {{ ref('weather_forecast_hourly') }}
group by location_id, forecast_at
having count(*) > 1

union all

select 'weather_forecast_daily', location_id, forecast_date::text, count(*)
from {{ ref('weather_forecast_daily') }}
group by location_id, forecast_date
having count(*) > 1
