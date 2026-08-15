# CELINE Pipelines

**CELINE Pipelines** is the reference repository providing **production-ready, open-data–based processing pipelines** built on top of the **CELINE data processing framework**.

Each pipeline is a **self-contained, reproducible application** that ingests, transforms, governs, and publishes datasets following CELINE standards for:
- data layers (raw / staging / silver / gold)
- governance & licensing
- OpenLineage metadata
- container-first execution
- cloud and on-prem deployments

This repository is part of the **CELINE EU project**.

Project website: https://celineproject.eu
Open-source tools & docs: https://celine-eu.github.io/

## Documentation

| Document | Description |
|---|---|
| [Pipeline Overview](https://celine-eu.github.io/projects/celine-pipelines/docs/pipeline-overview) | Pipeline anatomy, data layers, cross-pipeline contracts, `governance.yaml` |
| [Pipelines Reference](https://celine-eu.github.io/projects/celine-pipelines/docs/pipelines-reference) | Every pipeline: what it consumes, what it publishes, when it runs |
| [Local Runtime](https://celine-eu.github.io/projects/celine-pipelines/docs/local-runtime) | Running a pipeline in your terminal with the `celine-utils` CLI |
| [Testing](https://celine-eu.github.io/projects/celine-pipelines/docs/testing) | The test layers, the cross-pipeline cascade, current coverage |
| [Development](https://celine-eu.github.io/projects/celine-pipelines/docs/development) | Prerequisites, setup, Docker, releasing, adding a pipeline |

---

## What this repository contains

This repository hosts **end-to-end data pipelines** based on **open and public data sources**, including:

- **Meteorological data**
  - Open-Meteo (OM) — weather forecasts, historical archive, wind/heat risks, observations
  - MeteoTrentino (MT) — regional weather: stations, observations, forecasts, alerts
  - OpenWeatherMap (OWM)
  - Deutscher Wetterdienst (DWD — ICON-D2)
  - Copernicus Climate & Atmosphere Services (ERA5, CAMS)
  - Weather facade — one provider-neutral interface over all of the above
- **Geospatial open data**
  - OpenStreetMap (OSM)
  - Overture Maps — building footprints
  - Regional PV siting constraints
- **Photovoltaic analysis**
  - PV estimation — rooftop return on investment and installation planning
  - PV detection — existing installations identified from aerial imagery
- **Grid**
  - Wind and heat resilience overlays for the distribution network
- **REC and community data**
  - Metering interface — 15-minute and hourly readings, and the gap report over them
  - Italian CER virtual self-consumption settlement
  - Flexibility windows, settlement, gamification and CO2 impact
  - REC Registry and flexibility commitment mirrors

Each pipeline follows the same **canonical CELINE structure**:
- ingestion (Meltano / Singer taps)
- transformations (dbt: staging → silver → gold)
- orchestration (Prefect)
- governance metadata (`governance.yaml`)
- containerized execution (Docker / Skaffold)

Several pipelines read tables produced by **private ingestion pipelines that are not part
of this repository**. In each case the app's `sources.yml` is the published contract and
its `README.md` states the expected columns, so the pipeline can be run against
locally-created or synthetic data.

---

## Repository structure

```text
celine-pipelines/
├── apps/
│   ├── om/                          # Open-Meteo (weather, wind, heat, observations)
│   ├── mt/                          # MeteoTrentino regional weather
│   ├── owm/                         # OpenWeatherMap
│   ├── copernicus/                  # Copernicus Climate & Atmosphere (ERA5, CAMS)
│   ├── dwd/                         # DWD ICON-D2 weather model (paused)
│   ├── weather/                     # Provider-neutral weather facade
│   ├── osm/                         # OpenStreetMap ingestion & curation
│   ├── overture/                    # Overture Maps building footprints
│   ├── trentino_rooftops/           # PV siting constraints (regional open data)
│   ├── pv_estimation/               # Rooftop PV return-on-investment
│   ├── pv_detection/                # Existing PV detected from aerial imagery
│   ├── grid/                        # Grid wind & heat resilience overlays
│   ├── rec_metering/                # 15-min / hourly metering interface
│   ├── rec_it/                      # Italian CER virtual self-consumption
│   ├── rec_flexibility/             # Flexibility windows, settlement, gamification
│   ├── rec_registry/                # REC Registry data mirror
│   └── rec_flexibility_commitments/ # Flexibility commitments mirror
│
├── docs/               # Published documentation (celine-eu.github.io)
├── .agents/            # Agent knowledge, playbooks and plans
├── scripts/            # Release & utility scripts
├── docker-compose.yaml # Local stack: database, lineage, Prefect, one service per pipeline
├── Dockerfile.base     # Shared pipeline base image
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

Pipeline integration tutorial:
https://celine-eu.github.io/projects/celine-utils/docs/pipeline-tutorial

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

- Python >= 3.12
- Docker & Docker Compose
- `uv` 
- Prefect

### Setup

```bash
task setup
```

### Run a pipeline

Start the database, then run from inside the pipeline's own directory:

```bash
docker compose up datasets-db -d

cd apps/owm
source <(uv run celine-utils pipeline run envs)
uv run celine-utils pipeline run prefect
```

`pipeline run envs` prints the execution context as `export` lines — source it and bare
`dbt` and `meltano` commands work with no wrapper. See
[Local Runtime](https://celine-eu.github.io/projects/celine-pipelines/docs/local-runtime)
for the full command set, and
[Testing](https://celine-eu.github.io/projects/celine-pipelines/docs/testing) for how to
verify a change across the pipelines that depend on it.

---

## Versioning & releases

Each pipeline is **versioned independently** in `apps/<name>/version.txt`. CI watches
those files and publishes the matching image on change.

```bash
task pipeline:release:app -- osm --commit   # bump one pipeline
task pipeline:release:all                   # bump every pipeline
task pipeline:release:base                  # bump the shared base image
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
- Open-Meteo
- MeteoTrentino / Provincia Autonoma di Trento
- OpenStreetMap contributors
- OpenWeather Ltd.


---

## Pipeline Summary

Full detail — sources, layers, outputs, upstream contracts — in the
[Pipelines Reference](https://celine-eu.github.io/projects/celine-pipelines/docs/pipelines-reference).

| Pipeline | Source | Schedule | Key outputs |
|---|---|---|---|
| **om** (weather) | Open-Meteo | Daily 06:00 | 29 engineered weather features for energy forecasting |
| **om** (wind) | Open-Meteo | Every 4h | Wind speed/gust/direction on a 4.4 km Trentino grid |
| **om** (heat) | Open-Meteo | Daily 07:30 | Heat risk by altitude band (P90) |
| **om** (obs) | Open-Meteo | Every 2h | 15-min weather observations |
| **mt** | MeteoTrentino | Hourly | Regional stations, forecasts, alerts; weather contract tables |
| **owm** | OpenWeatherMap | Hourly | Per-location weather, plus a semantic observation view |
| **copernicus** | Copernicus | 4x daily | ERA5 reanalysis and CAMS atmospheric composition |
| **dwd** | DWD | paused | ICON-D2 model output (superseded by `om` wind) |
| **weather** | provider contract tables | Hourly | Deduplicated forecasts, alerts and observations per location |
| **osm** | OpenStreetMap | 4x daily | Thematic geospatial layers per covered area |
| **overture** | Overture Maps | Daily | Building footprints with stable identifiers |
| **trentino_rooftops** | Regional open data | Daily | Per-building PV siting eligibility |
| **pv_estimation** | Buildings + `celine-roi` | Weekly | Rooftop PV ROI, installation plans and rankings |
| **pv_detection** | Aerial imagery + vision model | Weekly | Buildings with PV already installed |
| **grid** | Grid topology + `om` forecasts | Daily, 15-min nowcast | Wind and heat risk overlays per line segment |
| **rec_metering** | Normalised meter readings | Every 10 min | 15-min and hourly metering interface, gap report |
| **rec_it** | `rec_metering` + registry + GSE | Every 15 min | Virtual self-consumption per device and community |
| **rec_flexibility** | `rec_metering` + forecasts | Daily 06:00 | Flexibility windows, settlement, gamification, CO2 |
| **rec_registry** | REC Registry API | Every 5 min | Community/member/asset mirror |
| **rec_flexibility_commitments** | Flexibility API | Every 15 min | Commitment mirror (90-day sliding window) |