# Open-Meteo Pipeline

## Overview

The **Open-Meteo pipeline** ingests hourly weather forecast and historical archive data from the **Open-Meteo API** for energy community forecasting use cases.

No API key is required -- Open-Meteo provides free access under CC-BY 4.0.

---

## Data sources

- Open-Meteo Forecast API (hourly weather variables)
- Open-Meteo Archive API (historical backfill)

Data is fetched from:
https://open-meteo.com/

---

## Output datasets

- **RAW**
  - Hourly weather observations and forecasts (om_weather)
- **STAGING**
  - Type-cast and deduplicated weather records
- **SILVER**
  - Physics-informed derived features: solar position, clear-sky index,
    cloud impact, heating/cooling degree hours, PV temperature correction,
    rolling snowfall accumulation

Licensing follows `CC-BY-4.0`.

---

## Execution

### Run once via Docker

```bash
docker compose up datasets-db -d
docker compose build pipeline-om
docker compose run --rm pipeline-om python3 -c "
from flows.pipeline import om_flow
om_flow(config={'mode': 'forecast'})
"
```

### Run as scheduled service (daily at 06:00)

```bash
docker compose up pipeline-om -d
```

This registers the Prefect flow with cron schedule `0 6 * * *`.

---

## Configuration & overrides

Customizable options:
- Location (latitude, longitude, timezone)
- Forecast and historical variables
- API endpoints and timeouts
- Physics thresholds (dbt vars with `om_` prefix)

See:
- `flows/config.yaml` -- pipeline parameters
- `dbt/dbt_project.yml` -- dbt variable overrides

---

## Contributing

Contributions may include:
- additional weather variables
- new derived indicators
- improved backfill or upsert logic

Ensure:
- licensing remains compatible
- derived datasets are documented in governance
