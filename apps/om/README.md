# Open-Meteo Pipeline

## Overview

The **Open-Meteo pipeline** ingests hourly weather forecast and historical archive data from the **Open-Meteo API** for energy community forecasting use cases.

No API key is required -- Open-Meteo provides free access under CC-BY 4.0.
To change from Best match model, modify the configuration on config.yaml
---

## Data sources

- Open-Meteo Forecast API (hourly weather variables)
- Open-Meteo Archive API (historical backfill)

Data is fetched from:
https://open-meteo.com/

---

## Pipeline architecture

```text
Open-Meteo API
     |
     v
[Fetch] ── forecast or historical ── Python (Prefect tasks)
     |
     v
[RAW] ── raw.om_weather ── verbatim API data
     |
     v
[STAGING] ── dbt ── type casting, deduplication
     |
     v
[SILVER] ── dbt ── 4 natural weather variables
     |
     v
[GOLD COMPUTE] ── Python ── 29 ML features (raw.om_weather_features)
     |
     v
[GOLD] ── dbt ── type casting, tests (ds_dev_gold.om_weather_features)
```

Both forecast and historical paths produce **identical columns** at every layer,
ensuring train/test consistency for ML models.

---

## Output datasets

### Silver layer (`ds_dev_silver.om_weather_hourly`)

The 4 natural weather variables, identical for forecast and historical:

| Column | Unit | Description |
|--------|------|-------------|
| `datetime` | timestamp | Hourly, Europe/Rome timezone |
| `shortwave_radiation` | W/m2 | Total solar radiation (GHI) |
| `cloud_cover` | % | Total cloud cover |
| `temperature_2m` | C | Air temperature at 2m |
| `precipitation` | mm | Total precipitation |

### Gold layer (`ds_dev_gold.om_weather_features`)

29 ML features for energy consumption forecasting:

**Temporal / Fourier (11 features)**

| Feature | Description |
|---------|-------------|
| `hour_sin`, `hour_cos` | Cyclical hour encoding (sin/cos of 2*pi*hour/24) |
| `dow_sin`, `dow_cos` | Cyclical day-of-week encoding (sin/cos of 2*pi*dow/7) |
| `annual_sin`, `annual_cos` | Annual cycle encoding (period = 8766 hours) |
| `semi_annual_sin`, `semi_annual_cos` | 6-month cycle encoding (period = 4383 hours) |
| `is_weekend` | 1 if Saturday or Sunday |
| `is_holiday` | 1 if Italian public holiday |
| `is_daylight` | 1 if hour between 6 and 20 (Italian time) |

**Temperature-derived (11 features)**

| Feature | Description |
|---------|-------------|
| `temperature_2m` | Direct pass-through (C) |
| `heating_degree_hour` | max(0, 18 - temperature_2m) |
| `temp_rolling_mean_24h` | 24h rolling mean |
| `temp_rolling_std_24h` | 24h rolling std |
| `temp_change_rate_3h` | (T[t] - T[t-3]) / 3 |
| `thermal_inertia_12h` | EWM with halflife=12h |
| `temp_gradient_24h` | T[t] - T[t-24] |
| `heating_degree_rolling_mean_24h` | 24h rolling mean of HDD |
| `cumulative_hdd_48h` | 48h rolling sum of HDD |
| `temp_x_hour_sin` | temperature_2m * hour_sin |
| `heating_x_night` | heating_degree_hour * is_night (hour >= 20 or <= 6) |

**Radiation-derived (3 features)**

| Feature | Description |
|---------|-------------|
| `shortwave_radiation` | Direct pass-through (W/m2) |
| `radiation_rolling_mean_24h` | 24h rolling mean |
| `radiation_x_daytime` | shortwave_radiation * is_daylight |

**Cloud-derived (2 features)**

| Feature | Description |
|---------|-------------|
| `cloud_cover` | Direct pass-through (%) |
| `cloud_cover_rolling_mean_24h` | 24h rolling mean |

**Precipitation (1 feature)**

| Feature | Description |
|---------|-------------|
| `precipitation` | Direct pass-through (mm) |

**Interaction (1 feature)**

| Feature | Description |
|---------|-------------|
| `weekend_x_hour_cos` | is_weekend * hour_cos |

Licensing follows `CC-BY-4.0`.

---

## Timezone handling

All datetime operations use Italian local time (`Europe/Rome`):

- The Open-Meteo API is called with `"timezone": "Europe/Rome"`
- Feature computation works directly on the local-time datetime column
- No UTC conversion happens at any stage
- DST transitions (2 hours/year) are not explicitly handled -- the model was trained with the same convention

---

## Execution

### Run once via Docker

```bash
# Ensure Postgres is up
docker compose up datasets-db -d
docker compose build pipeline-om
```

**Forecast mode** (daily use -- fetches 7-day forecast + 5 past days):

```bash
docker compose run --rm pipeline-om python3 -c "
from flows.pipeline import om_flow
om_flow(config={'mode': 'forecast'})
"
```

**Historical backfill** (fetches archive data in 3-month chunks):

```bash
docker compose run --rm pipeline-om python3 -c "
from flows.pipeline import om_flow
om_flow(config={'mode': 'historical', 'start_date': '2024-12-01'})
"
```

**Both** (historical backfill + current forecast in one run):

```bash
docker compose run --rm pipeline-om python3 -c "
from flows.pipeline import om_flow
om_flow(config={'mode': 'both', 'start_date': '2024-12-01'})
"
```

**With custom end date**:

```bash
docker compose run --rm pipeline-om python3 -c "
from flows.pipeline import om_flow
om_flow(config={'mode': 'historical', 'start_date': '2024-12-01', 'end_date': '2025-06-01'})
"
```

### Run as scheduled service (daily at 06:00)

```bash
docker compose up pipeline-om -d
```

This registers the Prefect flow with cron schedule `0 6 * * *`.

---

## Configuration

All pipeline parameters are in `flows/config.yaml`:

| Section | Key parameters |
|---------|---------------|
| `location` | latitude, longitude, timezone |
| `api` | forecast/archive URLs, timeout |
| `forecast` | forecast_days, past_days |
| `historical` | start_date, chunk_months |
| `variables` | hourly variable list (4 variables) |
| `silver` | silver table/schema (dbt output) |
| `gold` | gold table/schema (feature output) |
| `schedule` | cron expression, flow name |

---

## File structure

```text
apps/om/
├── flows/
│   ├── config.yaml       # Pipeline configuration
│   ├── features.py       # Gold-layer feature engineering (29 ML features)
│   └── pipeline.py       # Prefect tasks and flow definition
├── dbt/
│   ├── models/
│   │   ├── staging/      # stg_om_weather (type cast + dedup)
│   │   ├── silver/       # om_weather_hourly (4 weather variables)
│   │   └── gold/         # om_weather_features (29 ML features, type cast + tests)
│   ├── dbt_project.yml
│   └── profiles.yml
├── governance.yaml       # Dataset governance metadata
└── README.md
```

---

## Contributing

Contributions may include:
- additional ML features (update `features.py` and `SELECTED_FEATURES`)
- new locations (update `config.yaml`)
- improved imputation strategies
- additional dbt tests

Ensure:
- licensing remains compatible
- derived datasets are documented in governance
- feature parity between historical and forecast is maintained
