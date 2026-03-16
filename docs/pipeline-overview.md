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
