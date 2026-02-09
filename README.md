# CELINE Pipelines

**CELINE Pipelines** is the reference repository providing **production-ready, open-data–based processing pipelines** built on top of the **CELINE data processing framework**.

Each pipeline is a **self-contained, reproducible application** that ingests, transforms, governs, and publishes datasets following CELINE standards for:
- data layers (raw / staging / silver / gold)
- governance & licensing
- OpenLineage metadata
- container-first execution
- cloud and on-prem deployments

This repository is part of the **CELINE EU project**.

🌍 Project website: https://celineproject.eu  
🛠 Open-source tools & docs: https://celine-eu.github.io/

---

## What this repository contains

This repository hosts **end-to-end data pipelines** based on **open and public data sources**, including:

- **Meteorological data**
  - OpenWeatherMap (OWM)
  - Deutscher Wetterdienst (DWD – ICON-D2)
  - Copernicus Climate & Atmosphere Services (ERA5, CAMS)
- **Geospatial open data**
  - OpenStreetMap (OSM)

Each pipeline follows the same **canonical CELINE structure**:
- ingestion (Meltano / Singer taps)
- transformations (dbt: staging → silver → gold)
- orchestration (Prefect)
- governance metadata (`governance.yaml`)
- containerized execution (Docker / Skaffold)

---

## Repository structure

```text
celine-pipelines/
├── apps/
│   ├── copernicus/     # Copernicus Climate & Atmosphere pipelines
│   ├── dwd/            # DWD ICON-D2 weather model
│   ├── om/             # Open-Meteo weather pipeline (forecast + historical + ML features)
│   ├── osm/            # OpenStreetMap ingestion & curation
│   └── owm/            # OpenWeatherMap pipelines
│
├── scripts/            # Release & utility scripts
├── skaffold.yaml       # Container build configuration
├── taskfile.yaml       # Developer & CI tasks
├── pyproject.toml
└── README.md
```

Each subfolder under `apps/` is a **fully independent pipeline application** with its own:
- Prefect flows
- dbt project
- Meltano configuration
- governance rules
- versioning

---

## Pipeline architecture (CELINE standard)

All pipelines implement the same layered data model:

| Layer    | Purpose |
|---------|---------|
| **RAW** | Verbatim ingested data |
| **STAGING** | Technical normalization |
| **SILVER** | Enriched, curated datasets |
| **GOLD** | Shareable, domain-ready datasets |

Governance rules (license, access level, attribution, retention) are declared **explicitly per dataset** in `governance.yaml`.

---

## Adding a new pipeline

To create and integrate a new pipeline, follow the official tutorial:

👉 **Pipeline integration tutorial**  
https://celine-eu.github.io/projects/celine-utils/docs/pipeline-tutorial/

The tutorial covers:
- creating a new pipeline skeleton
- defining Prefect flows
- configuring Meltano & dbt
- adding governance metadata
- local development and container execution

All pipelines in this repository are built following that guide.

---

## Local development

### Prerequisites

- Python ≥ 3.11
- Docker & Docker Compose
- `uv` 
- Prefect

### Setup

```bash
task setup
```

### Run a pipeline

Example (OpenWeatherMap):

```bash
task pipeline:owm:run
```

---

## Versioning & releases

Each pipeline is **versioned independently**.

Example:
```bash
task pipeline:osm:release
```

---

## Governance & licensing

All datasets are governed explicitly:
- licenses are respected and propagated
- attribution is enforced
- access levels are declared (`internal`, `external`, `restricted`)
- ingestion artifacts are never exposed

See each pipeline’s `governance.yaml` for authoritative rules.

---

## Related repositories

- **celine-utils** – shared pipeline framework  
  https://github.com/celine-eu/celine-utils
- **CELINE documentation portal**  
  https://celine-eu.github.io/

---

## License


Copyright >=2025 Spindox Labs

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.


---

## Acknowledgements

This work is part of the **CELINE project**, funded under the European Union framework, and builds upon multiple open data initiatives including:
- Copernicus Programme
- Deutscher Wetterdienst (DWD)
- OpenStreetMap contributors
- OpenWeather Ltd.


---

## OM pipeline (Open-Meteo)

The Open-Meteo pipeline fetches hourly weather data (forecast + historical archive),
transforms it through staging and silver layers (4 natural weather variables),
then computes 29 ML features in a gold layer for energy consumption forecasting.

### Pipeline layers

| Layer | Description |
|-------|-------------|
| **RAW** | Verbatim API data (`raw.om_weather`) |
| **STAGING** | Type-cast and deduplicated records (`ds_dev_staging.stg_om_weather`) |
| **SILVER** | 4 natural weather variables: `shortwave_radiation`, `cloud_cover`, `temperature_2m`, `precipitation` (`ds_dev_silver.om_weather_hourly`) |
| **GOLD** | 29 ML features: temporal/Fourier encodings, rolling stats, thermal dynamics, interaction features (`ds_dev_gold.om_weather_features`) |

### Run in Docker

```bash
# Forecast mode (daily use)
docker compose up datasets-db -d
docker compose build pipeline-om
docker compose run --rm pipeline-om python3 -c "
from flows.pipeline import om_flow
om_flow(config={'mode': 'forecast'})
"
```

```bash
# Historical backfill
docker compose run --rm pipeline-om python3 -c "
from flows.pipeline import om_flow
om_flow(config={'mode': 'historical', 'start_date': '2024-12-01'})
"
```

```bash
# Both historical + forecast
docker compose run --rm pipeline-om python3 -c "
from flows.pipeline import om_flow
om_flow(config={'mode': 'both', 'start_date': '2024-12-01'})
"
```

Or start as a scheduled service (daily at 06:00):
```bash
docker compose up pipeline-om -d
```