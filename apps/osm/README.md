# OpenStreetMap Pipeline

## Overview

The **OSM pipeline** ingests and curates **OpenStreetMap** geospatial data for selected regions, producing thematic datasets for mobility, tourism, infrastructure, and services.

---

## Data sources

- OpenStreetMap planet extracts
- Regional queries via Overpass-compatible tooling

License: **ODbL 1.0**

---

## Output datasets

- **RAW**
  - GeoJSON extracts
- **STAGING**
  - Normalized geometries
- **SILVER**
  - Thematic layers (parking, hospitality, EV charging, etc.)
- **GOLD**
  - Externally shareable geospatial datasets

Attribution is mandatory and enforced.

---

## Execution & Docker image

Docker image:
```
ghcr.io/celine-eu/pipeline-osm
```

Run locally:
```bash
task pipeline:osm:run
```

---

## Configuration & overrides

Custom use cases can adjust:
- Areas of interest
- OSM tag filters
- Output formats

See:
- `flows/osm_config.yaml`

---

## Contributing

New contributions should:
- respect ODbL requirements
- preserve attribution
- document new thematic layers

Do not open PRs adding new regions, override `flows/osm_config.yaml` to adapt to your needs instead.
