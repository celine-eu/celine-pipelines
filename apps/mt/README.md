# MeteoTrentino Pipeline

## Overview

The **MeteoTrentino (MT) pipeline** ingests regional weather data from **MeteoTrentino** — the meteorological service of the Autonomous Province of Trento.

It collects station observations, hourly and daily forecasts, weather alerts, and reference data on an **hourly schedule**.

---

## Data sources

- MeteoTrentino APIs (ASMX, meteo.report, API Manager)

Data is sourced from:
https://www.meteotrentino.it

---

## Output datasets

- **RAW**
  - Sky conditions, alerts, station registry, observations, forecast locations, hourly & daily forecasts
- **STAGING**
  - Type-cast and normalized views
- **SILVER**
  - Deduplicated station observations (incremental), curated forecasts, active alerts
- **GOLD**
  - `mt_observations_current` — latest observation per active station
  - `mt_stations` — active station registry
  - `mt_alerts_active` — non-expired deduplicated alerts
  - `mt_forecast_hourly` — 3-hour forecasts with location enrichment
  - `mt_forecast_daily` — daily forecasts with location enrichment

Licensing follows `CC-BY-4.0`.

---

## Execution & Docker image

Docker image:
```
ghcr.io/celine-eu/pipeline-mt
```

Run locally:
```bash
task pipeline:mt:run
```

---

## Configuration & overrides

Schedule: hourly at `:05` (`5 * * * *`)

Customizable options:
- Station and location scope (via Meltano tap)
- Observation lookback window
- Retention policies

See:
- `flows/config.yaml`
- `meltano/meltano.yml`
- dbt model configurations

---

## Contributing

Contributions may include:
- additional MeteoTrentino data streams
- new derived weather indicators
- improved deduplication or cleanup logic

Ensure:
- CC-BY-4.0 attribution is preserved
- derived datasets are documented in governance
