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