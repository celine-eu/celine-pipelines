# Development

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Python | ≥ 3.11 | Pipeline runtime |
| `uv` | latest | Dependency management |
| Docker + Docker Compose | latest | Container execution |
| Prefect | ≥ 2.x | Flow orchestration |
| Task | latest | Developer task runner |

Install Task: https://taskfile.dev

## Setup

```bash
task setup
```

This installs all Python dependencies and sets up the local development environment.

## Running Individual Pipelines

```bash
# Open-Meteo (forecast mode)
task pipeline:om:run

# Open-Meteo (historical backfill)
task pipeline:om:run -- mode=historical start_date=2024-12-01

# DWD
task pipeline:dwd:run

# OpenWeatherMap
task pipeline:owm:run

# Copernicus ERA5
task pipeline:copernicus:run

# OpenStreetMap
task pipeline:osm:run
```

## Running in Docker

```bash
# Start the database
docker compose up datasets-db -d

# Build a specific pipeline
docker compose build pipeline-om

# Run in Docker
docker compose run --rm pipeline-om python3 -c "
from flows.pipeline import om_flow
om_flow(config={'mode': 'forecast'})
"

# Run as scheduled service (daily)
docker compose up pipeline-om -d
```

## Releasing a Pipeline

Each pipeline is versioned independently. To tag and release a new version:

```bash
task pipeline:om:release
task pipeline:osm:release
```

This bumps the version, tags the Git commit, and triggers the CI build for the Docker image.

## Adding a New Pipeline

Follow the official pipeline integration tutorial:

https://celine-eu.github.io/projects/celine-utils/docs/pipeline-tutorial.md

The tutorial covers:
- Creating a pipeline skeleton with `celine-utils pipeline init`
- Defining Prefect flows
- Configuring Meltano taps and dbt projects
- Adding governance metadata
- Local and container execution

## Environment Variables

Each pipeline reads its configuration from environment variables. Common variables:

| Variable | Description |
|---|---|
| `DATABASE_URL` | Target PostgreSQL URL for dbt and dataset writes |
| `MARQUEZ_URL` | OpenLineage backend URL (optional) |
| `PREFECT_API_URL` | Prefect server URL (optional, for remote execution) |

Pipeline-specific variables (e.g., `OWM_API_KEY`, `CDSAPI_KEY`) are documented in each pipeline's `README.md`.
