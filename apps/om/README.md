# Open-Meteo Pipeline

## Overview

The **Open-Meteo pipeline** ingests hourly weather forecast data from the **Open-Meteo API** via **Meltano** (`tap-openmeteo`) for energy community forecasting use cases.

No API key is required -- Open-Meteo provides free access under CC-BY 4.0.
To change the weather model, modify the configuration in `meltano/meltano.yml`.

---

## Data sources

- Open-Meteo Forecast API (hourly weather variables, DWD ICON-D2 model)

Data is fetched from:
https://open-meteo.com/

---

## Pipeline architecture

```text
Open-Meteo API
     |
     v
[Meltano] ── tap-openmeteo -> target-postgres
     |
     v
[RAW] ── raw.om_weather ── verbatim API data
     |
     v
[STAGING] ── dbt ── type casting (time -> datetime), deduplication
     |
     v
[SILVER] ── dbt ── 7 weather variables (hourly)
     |
     v
[GOLD COMPUTE] ── Python ── 29 ML features + 15 PV features
     |
     v
[GOLD] ── dbt ── type casting, tests (ds_dev_gold.om_weather_features)
```

---

## Output datasets

### Silver layer (`ds_dev_silver.om_weather_hourly`)

7 weather variables:

| Column | Unit | Description |
|--------|------|-------------|
| `datetime` | timestamp | Hourly, Europe/Rome timezone |
| `shortwave_radiation` | W/m2 | Total solar radiation (GHI) |
| `direct_radiation` | W/m2 | Direct beam solar radiation |
| `diffuse_radiation` | W/m2 | Scattered solar radiation |
| `global_tilted_irradiance` | W/m2 | Global tilted irradiance for PV |
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

### Gold meters layer (`ds_dev_gold.om_weather_features_meters`)

15 PV/solar features for energy metering:

| Feature | Description |
|---------|-------------|
| `hour_sin`, `hour_cos` | Cyclical hour encoding |
| `day_of_week`, `month` | Calendar features |
| `is_weekend`, `is_daylight` | Binary flags |
| `global_tilted_irradiance` | PV panel irradiance (W/m2) |
| `shortwave_radiation` | GHI (W/m2) |
| `cloud_cover` | Cloud cover (%) |
| `temperature_2m` | Temperature (C) |
| `clearsky_index` | Ratio of actual to theoretical clear-sky GHI |
| `effective_solar_pv` | direct_radiation + 0.9 * diffuse_radiation |
| `heating_degree`, `cooling_degree` | Thermal comfort indices |
| `theoretical_prod` | GTI * effective_solar_pv |

Licensing follows `CC-BY-4.0`.

---

## Timezone handling

All datetime operations use Italian local time (`Europe/Rome`):

- The tap-openmeteo config sets `timezone: "Europe/Rome"`
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

**Daily mode** (fetches 48h forecast + 120h past):

```bash
docker compose run --rm pipeline-om python3 -c "
from flows.pipeline import om_flow
om_flow()
"
```

### Run as scheduled service (daily at 06:00)

```bash
docker compose up pipeline-om -d
```

This registers the Prefect flow with cron schedule `0 6 * * *`.

---

## Configuration

Pipeline configuration is split between two files:

### `meltano/meltano.yml` -- Extraction config

| Section | Key parameters |
|---------|---------------|
| `locations` | name, latitude, longitude, timezone |
| `forecast_hours` / `past_hours` | Time window (48h forecast, 120h past) |
| `models` | Weather model selection (icon_d2) |
| `hourly_variables` | 7 weather variables |
| `streams_to_sync` | weather_hourly |
| `stream_maps` | weather_hourly -> om_weather (table alias) |

### `flows/config.yaml` -- Table mappings and schedule

| Section | Key parameters |
|---------|---------------|
| `silver` | silver table/schema (dbt output) |
| `gold_raw` / `gold_raw_meters` | raw gold tables (Python feature output) |
| `gold` / `gold_meters` | gold table/schema (dbt output) |
| `schedule` | cron expression, flow name |

---

## File structure

```text
apps/om/
├── flows/
│   ├── config.yaml       # Table mappings and schedule
│   ├── features.py       # Gold-layer feature engineering (29 + 15 features)
│   └── pipeline.py       # Prefect tasks and flow definition
├── meltano/
│   ├── meltano.yml       # Meltano extractor/loader config (tap-openmeteo)
│   └── .gitignore        # Ignores .meltano/ runtime
├── dbt/
│   ├── models/
│   │   ├── staging/      # stg_om_weather (time->datetime, type cast, dedup)
│   │   ├── silver/       # om_weather_hourly (7 weather variables)
│   │   └── gold/         # om_weather_features (type cast + tests)
│   ├── macros/
│   │   └── cleanup_om_weather.sql  # Retention-based raw data cleanup
│   ├── dbt_project.yml
│   └── profiles.yml
├── governance.yaml       # Dataset governance metadata
└── README.md
```

---

## Contributing

Contributions may include:
- additional ML features (update `features.py` and `SELECTED_FEATURES`)
- new locations (add to `meltano/meltano.yml` `locations` array)
- improved imputation strategies
- additional dbt tests

Ensure:
- licensing remains compatible
- derived datasets are documented in governance
- feature parity between historical and forecast is maintained
