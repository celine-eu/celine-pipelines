# DWD Pipeline

## Overview

The **DWD pipeline** processes numerical weather prediction data from the **Deutscher Wetterdienst (DWD)**, specifically the **ICON-D2** high-resolution model.

It is designed for **near-real-time weather forecasting** and **risk assessment** use cases.

---

## Data sources

- ICON-D2 forecast model (open data)

Data is downloaded from:
https://opendata.dwd.de/

---

## Output datasets

- **RAW**
  - Full ICON-D2 GRIB forecasts
- **STAGING**
  - Parsed meteorological variables
- **SILVER**
  - Enriched wind and gust datasets
- **GOLD**
  - Shareable weather indicators (e.g. gust alerts)

Licensing follows `dl-de/by-2-0`.

---

## Execution & Docker image

Docker image:
```
ghcr.io/celine-eu/pipeline-dwd
```

Run locally:
```bash
task pipeline:dwd:run
```

---

## Configuration & overrides

Customizable options:
- Geographic bounding boxes
- Forecast steps & variables
- Retention and cleanup policies

See:
- `flows/config.yaml`
- dbt cleanup operations

---

## Contributing

Contributions may include:
- additional ICON products
- new derived indicators
- improved cleanup or validation logic

Ensure:
- licensing remains compatible
- derived datasets are documented in governance
