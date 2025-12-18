# Copernicus Pipeline

## Overview

The **Copernicus pipeline** ingests and processes climate and atmospheric datasets from the **Copernicus Climate Change Service (C3S)** and **Copernicus Atmosphere Monitoring Service (CAMS)**.

It focuses on **ERA5 reanalysis** and **CAMS solar radiation and air quality datasets**, transforming raw GRIB/CSV files into curated datasets suitable for analytics and downstream CELINE applications.

---

## Data sources

- ERA5 single-level reanalysis (daily & monthly)
- CAMS global reanalysis (monthly)
- CAMS solar radiation time series

All datasets are retrieved using official Copernicus APIs.

---

## Output datasets

The pipeline exports:

- **RAW**
  - Verbatim ERA5 and CAMS datasets
- **STAGING**
  - Normalized meteorological variables
- **SILVER**
  - Curated, analysis-ready climate indicators
- **GOLD**
  - Domain-specific aggregates (where applicable)

Dataset governance, licensing, and attribution are defined in `governance.yaml`.

---

## Execution & Docker image

Docker image:
```
ghcr.io/celine-eu/pipeline-copernicus
```

Run locally:
```bash
task pipeline:copernicus:run
```

---

## Configuration & overrides

Custom deployments can override:
- CDS / ADS API keys
- Spatial bounding boxes
- Temporal ranges
- Storage backend (local FS or S3)

See:
- `flows/cds_config.yaml`
- environment variables in `.env.example`

---

## Contributing

To propose changes:
1. Fork the repository
2. Modify configs, dbt models, or Prefect flows
3. Update governance if datasets change
4. Submit a pull request with documentation

