-- On a daily forecast, min <= temperature <= max.
--
-- The three columns are cast independently from three separate upstream fields.
-- Nothing ties them together, so an upstream field rename — or a reordering
-- that silently swaps minimum and maximum — produces three plausible numbers in
-- the wrong relationship. That survives every type check and every range check,
-- because each value on its own is a perfectly ordinary temperature.
--
-- The consequence is not cosmetic: `weather_forecast_daily` carries these
-- straight through to the webapp, where an inverted range reads as a forecast
-- for a day that is colder at its peak than at its trough.
--
-- Rows where a bound is null are skipped: not every location publishes both.

select
    location_id,
    forecast_at,
    temperature_min_c,
    temperature_c,
    temperature_max_c
from {{ ref('mt_forecasts_daily') }}
where temperature_min_c > temperature_max_c
   or temperature_c < temperature_min_c
   or temperature_c > temperature_max_c
