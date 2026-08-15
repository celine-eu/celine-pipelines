-- Rows the weather facade cannot tell apart must carry the same forecast.
--
-- `apps/weather` resolves a seed location to a provider row by horizontal
-- distance, then by elevation difference. Its discriminating key is therefore
-- (lat, lon, elevation_m) — NOT location_id, which is provider-specific and
-- which the facade never sees.
--
-- So two contract rows sharing all three, for the same instant, are
-- indistinguishable to the consumer. That is fine when they say the same thing:
-- MeteoTrentino publishes some places under two names — `Povo` and
-- `Trento - collina` share a coordinate, an elevation, and every forecast value
-- — and picking either is correct.
--
-- It is a defect when they disagree, because then the facade is choosing
-- between different answers on no basis at all, and the choice can flip between
-- runs with no change in the data.
--
-- Expected: zero rows. This was NOT zero before `elevation_m` was added to the
-- contract — mountain massifs publish a vertical profile at one representative
-- lat/lon, so four altitude bands collided on (lat, lon) alone and spanned
-- ~10 °C. Elevation is what separates them; this test is what keeps that true.
--
-- If it fires, the fix is upstream or in the contract, never a `distinct` here:
-- collapsing the rows would discard a real forecast.

select
    lat,
    lon,
    elevation_m,
    forecast_at,
    count(*)                      as indistinguishable_rows,
    count(distinct location_id)   as distinct_locations,
    count(distinct temperature_c) as distinct_temperatures,
    min(temperature_c)            as min_temperature_c,
    max(temperature_c)            as max_temperature_c
from {{ ref('mt_weather_forecast_hourly') }}
group by lat, lon, elevation_m, forecast_at
having count(distinct temperature_c) > 1
