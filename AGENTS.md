## Repository role

This repository implements the CELINE data ingestion and transformation backbone. It is responsible for collecting heterogeneous source data, preserving provenance, applying structural and semantic refinement, and materialising governed data products for downstream consumption across the platform.

It follows a medallion pattern: `raw`, `staging`/`silver` normalization, `gold` stable interfaces.

`celine-utils` is used in prefect pipelines to track lineage, load `governance.yaml` information

This repository is expected to preserve the integrity of CELINE’s layered data model, quality gates, governance metadata, and lineage records. Changes here can affect nearly every downstream service, especially the Dataset API, Digital Twin, AI components, and user-facing applications.

## Structure

### Base image

The repo contains a base image `Dockerfile.base` integrating `celine-utils` and providing the common layers for pipeline "apps" to work. It assumes a python setup managed via uv.
`version-base.txt` tracks the base pipeline version, mapped to the image built via GH actions.

### Pipeline Apps

In `apps/**` each folder is a self-contained pipeline. The structure is 

- `dbt` dbt related models, seed, macros etc. Pipelines are generally incremental, cold storage is managed with `pg_freezer` that schedules table data removal (based on a timestamp) after safely storing in parquet on s3/minio.
- `flows` prefect flows implementation, leveraging on `celine-utils` wrappers
- `meltano` meltano extractor and loaders configuration, used to create `raw` tables
- `governance.yaml` is a structured format to track processed datasets ownerships, lineage and dataset exposure details via `dataset-api`
- `requirements.txt` optional, per pipeline, dependencies installed in the container uv env
- `version.txt` tracks the pipeline version, mapped to the image built via GH actions.

### Community & REC Pipelines

These pipelines process Renewable Energy Community (REC) data. They consume upstream silver tables via configurable source schemas (`CELINE_SILVER_SCHEMA`, `CELINE_GOLD_SCHEMA` env vars) to decouple from deployment-specific data sources.

- `grid` — CIM-inspired grid resilience analysis (wind/heat risks, network topology). Requires PostGIS. Upstream: silver grid topology + om weather forecasts.
- `rec_metering` — Aggregate and normalize REC meter readings to 15-min and hourly intervals. Upstream: silver meters_data_normalized.
- `rec_it` — Italian-specific REC models (GSE virtual consumption incentives, cabine primarie). Upstream: rec_metering gold + GSE open data (meltano).
- `rec_flexibility` — Flexibility indicators, gamification, CO2 savings, settlement. Includes Python tasks (baselines, streaks). Upstream: rec_metering silver + meter forecasts + flexibility commitments.

### Governance

`governance.yaml` must be available in every pipeline app. It define information for governance and lineage.

Each pipeline must list only the datasets it handles, duplications across pipelines are not allowed (eg. define the source from another pipeline)

The full schema is available in `celine-utils/schema/governance.schema.json`

A file called `owners.yaml` defines the `ownership` values mapping. See `dataset-api/owners.yaml` (and `owners.local.yaml` if exists) for a reference.

### Docker

In `docker-compose.yaml` each pipeline has its own container for local replication. Ensure pipelines in `apps/*` have it, following the established pattern.

You can use local docker postgres at `postgres:securepassword123@host.docker.internal:15432/datasets` with pgsql or via docker exec.
Operate read only, ask for permission otherwise. Pipeline should be self contained, and ensure the table is created at runtime.