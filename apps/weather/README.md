# Weather Pipeline

Unified weather facade that aggregates forecasts, alerts, and current observations from multiple providers into stable gold tables consumed by the digital-twin and celine-webapp.

## Architecture

The pipeline follows the **contract table** pattern (same as `heat_daily_obs` in the grid pipeline):

- **Provider pipelines** (e.g., `apps/mt`) produce contract tables with generic aliases (`weather__forecast_hourly`, `weather__forecast_daily`, `weather__alerts`, `weather__current`) in the gold schema.
- **This pipeline** reads those contract tables as sources and produces consumer-facing gold tables with spatial deduplication (nearest provider location per seed entry).

Adding a new provider means the provider pipeline writes to the same contract tables. This pipeline requires **no changes**.

## Gold outputs

| Table | Description |
|---|---|
| `weather_forecast_hourly` | Sub-daily forecasts per configured location (incremental) |
| `weather_forecast_daily` | Daily forecast summaries per configured location (incremental) |
| `weather_alerts_active` | Active weather alerts per configured location |
| `weather_current` | Latest observation per configured location (nearest station) |

## Location configuration

Locations are managed via the dbt seed `data/weather_locations.csv`:

```csv
location_id,provider,location_name,lat,lon
it_folgaria,mt,Folgaria,45.9167,11.1667
```

The `provider` column is used only for alert scoping (regional alerts are assigned to locations matching their provider). Forecast and current observations are routed purely by spatial proximity.

## Cleanup

The `cleanup_weather_forecasts` macro (called at the end of each pipeline run) deletes forecast rows older than 30 days to prevent table growth.

## Schedule

Runs hourly at `:15`, after provider pipelines complete (MT runs at `:05`).
