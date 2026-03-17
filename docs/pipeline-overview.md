# Pipeline Overview

## Standard Pipeline Anatomy

Every CELINE pipeline is a self-contained application following a canonical structure. Each pipeline lives under `apps/<name>/` and bundles all the tooling needed to ingest, transform, govern, and publish a dataset.

## Data Layers

| Layer | Purpose |
|---|---|
| **RAW** | Verbatim data as received from the source — no transformations |
| **STAGING** | Technical normalization: type casting, deduplication, field renaming |
| **SILVER** | Enriched and curated datasets, domain-ready for analysis |
| **GOLD** | Shareable, publication-ready datasets with full governance metadata |

Each layer is a set of dbt models (or Meltano taps for RAW). Governance rules apply from STAGING onward.

## Tooling Per Layer

| Layer | Tool | Description |
|---|---|---|
| RAW | Meltano / Singer tap | Source API ingestion |
| STAGING → GOLD | dbt | SQL transformations and tests |
| Orchestration | Prefect | Flow execution, scheduling, retries |
| Lineage | OpenLineage | Automatic metadata emission per run |
| Governance | `governance.yaml` | Dataset license, access level, attribution |

## governance.yaml in Pipelines

Each pipeline includes a `governance.yaml` at the app root. This file declares:

```yaml
datasets:
  - name: om_weather_hourly
    namespace: ds_dev_silver
    license: ODbL-1.0
    access_level: internal
    attribution: "Open-Meteo contributors"
    retention_days: 365
    tags:
      - weather
      - meteorology
```

Rules are resolved using pattern matching. The `celine-utils` governance engine injects these rules into OpenLineage events at run time.

## Pipeline Container Structure

Each pipeline application is containerized:

```
apps/<name>/
  flows/
    pipeline.py       # Prefect flow definition
  models/
    staging/          # dbt staging models
    silver/           # dbt silver models
    gold/             # dbt gold models
  meltano.yml         # Meltano configuration
  governance.yaml     # Dataset governance rules
  pyproject.toml
  Dockerfile
```

The container entrypoint runs the Prefect flow. Pipelines can also run as scheduled Prefect deployments.

## Multi-Flow Pipelines

A single app can host multiple Prefect flows that share the same Docker image
but run on independent schedules. Each flow gets its own `pipeline_<name>.py`
and `config_<name>.yaml`. dbt models are organized in **subdirectories** within
the standard layer folders and selected using **dbt tags** (comma = intersection):

```
apps/om/
  flows/
    pipeline.py           # om-flow (weather)
    pipeline_wind.py      # om-wind-flow (wind grid)
    config.yaml
    config_wind.yaml
  dbt/models/
    staging/
      stg_om_weather.sql            # weather (no tag)
      wind/
        stg_om_wind.sql             # wind (tag: wind)
    silver/
      om_weather_hourly.sql
      wind/
        om_wind_hourly.sql
    gold/
      om_weather_features.sql
      wind/
        om_wind_gusts.sql
```

Each flow selects only its own models via dbt tags:

```python
# wind pipeline uses intersection: layer AND tag
dbt_run("-s staging,tag:wind", cfg)   # only wind staging
dbt_run("-s silver,tag:wind", cfg)    # only wind silver
dbt_run("-s gold,tag:wind", cfg)      # only wind gold
dbt_run("test -s tag:wind", cfg)      # only wind tests
```

Tags are defined in per-directory schema YAML files:

```yaml
# dbt/models/silver/wind/wind_schema.yml
models:
  - name: om_wind_hourly
    config:
      tags: ["wind"]
```

This pattern avoids cross-flow interference: the weather flow's `dbt_run("staging", cfg)`
runs all staging models (including wind), but wind models are harmless when run by the
weather flow (views are cheap, incremental tables skip when no new data).

**Note on dbt selectors:** In dbt, comma within `--select` is **intersection** (AND),
while space-separated or multiple `-s` flags is **union** (OR). This is the opposite
of what most people expect. Verified in dbt-core source (`dbt/graph/cli.py`).
