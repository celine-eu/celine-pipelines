# OpenWeatherMap Pipeline

## Overview

The **OpenWeatherMap pipeline** ingests weather observations and forecasts using the **OpenWeatherMap One Call API** and produces curated weather datasets.

It supports multiple locations and time resolutions.

---

## Data sources

- OpenWeatherMap One Call API 3.0

License constraints apply (non-commercial sharing).

---

## Output datasets

- **RAW**
  - API responses
- **STAGING**
  - Normalized time series
- **SILVER**
  - Internal analytical datasets
- **GOLD**
  - Shareable aggregated weather indicators

Licensing is enforced per dataset.

---

## Execution & Docker image

Docker image:
```
ghcr.io/celine-eu/pipeline-owm
```

Run locally:
```bash
task pipeline:owm:run
```

---

## Configuration & overrides

Customizable options:
- Locations (lat/lon or city)
- Update frequency
- Units and aggregation logic

Configured via:
- Meltano configs
- Environment variables

---

## Contributing

Due to API license constraints:
- no raw data redistribution
- derived datasets only

Contributions should focus on:
- transformations
- indicators
- metadata improvements
