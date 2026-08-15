# Weather Pipeline

Unified weather facade that aggregates forecasts, alerts, and current observations from multiple providers into stable gold tables consumed by the digital-twin and celine-webapp.

## Architecture

The pipeline follows the **contract table** pattern (same as `heat_daily_obs` in the grid pipeline):

- **Provider pipelines** (e.g., `apps/mt`) produce contract tables with generic aliases (`weather__forecast_hourly`, `weather__forecast_daily`, `weather__alerts`, `weather__current`) in the gold schema.
- **This pipeline** reads those contract tables as sources and produces consumer-facing gold tables with spatial deduplication (nearest provider location per seed entry).

### Matching a seed location to a provider row

Candidates within 0.1 degrees (0.15 for observations) are ranked **lexicographically**:
horizontal distance, then `abs(elevation difference)`, then the provider's own id.

The elevation term is not a refinement — it is load-bearing. A coordinate is not a unique
forecast point: providers publish mountain massifs as a vertical profile, one
representative lat/lon with a separate location per altitude band. Those tie at distance
zero, and without elevation the winner was whichever row the database returned first, a
~10 °C swing between runs.

Every provider contract table must therefore expose `lat`, `lon` and `elevation_m`. A
provider that omits `elevation_m` still matches; it just cannot have ties between its own
rows broken meaningfully.

Adding a new provider means the provider pipeline writes to the same contract tables. This pipeline requires **no changes**.

## Gold outputs

| Table | Description |
|---|---|
| `weather_forecast_hourly` | Sub-daily forecasts per configured location (incremental) |
| `weather_forecast_daily` | Daily forecast summaries per configured location (incremental) |
| `weather_alerts_active` | Active weather alerts per configured location |
| `weather_current` | Latest observation per configured location (nearest station) |

## Location configuration

Locations are managed via the dbt seed `seed/weather_locations.csv`:

```csv
location_id,provider,location_name,lat,lon,elevation_m
it_folgaria,mt,Folgaria,45.9167,11.1667,1166
fi_lappeenranta,owm,Lappeenranta,61.050009,28.18739,
```

The `provider` column is used only for alert scoping (regional alerts are assigned to
locations matching their provider). Forecasts and current observations are routed by
spatial proximity, then by elevation.

`elevation_m` is **optional but recommended**. Give it whenever the location sits in
terrain where altitude matters: it is what picks the right altitude band when several
provider points share a coordinate. Leave it blank and the location still matches — the
tie-break simply falls through to a stable-but-arbitrary ordering, which is the old
behaviour. `fi_lappeenranta` and `es_valencia` are blank above because no elevation was
sourced for them; fill them in if either starts receiving provider data.

## Cleanup

The `cleanup_weather_forecasts` macro (called at the end of each pipeline run) deletes forecast rows older than 30 days to prevent table growth.

## Schedule

Runs hourly at `:15`, after provider pipelines complete (MT runs at `:05`).
