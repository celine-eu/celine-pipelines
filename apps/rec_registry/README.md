# REC Registry Pipeline

## Overview

The **REC Registry pipeline** mirrors community membership data from the **CELINE REC Registry API** into a PostgreSQL raw table.

It performs a **full-replace refresh every 5 minutes**, providing a stable source of active members, their grid areas, topology nodes, delivery points, and meter sensors.

---

## Data sources

- CELINE REC Registry API (internal, OIDC-authenticated)

License: Proprietary.

---

## Output datasets

- **RAW**
  - `rec_registry_mirror` — full-replace mirror of active community members (one row per user/community pair)

No dbt transformation layers are included in this pipeline. The raw table serves as a source for downstream dbt pipelines computing virtual self-consumption, billing, and community analytics.

---

## Execution & Docker image

Docker image:
```
ghcr.io/celine-eu/pipeline-rec-registry
```

Run locally:
```bash
task pipeline:rec_registry:run
```

---

## Configuration & overrides

Schedule: every 5 minutes (`*/5 * * * *`)

Customizable options:
- REC Registry API URL (`CELINE_REC_REGISTRY_URL`)
- OIDC credentials (`CELINE_OIDC_CLIENT_ID`, `CELINE_OIDC_CLIENT_SECRET`)

See:
- `flows/config.yaml`

---

## Contributing

Contributions may include:
- additional member attributes or community metadata
- transformation layers (dbt silver/gold)
- improved refresh or deduplication logic

Ensure:
- access restrictions are respected (internal, contract-required)
- derived datasets are documented in governance
