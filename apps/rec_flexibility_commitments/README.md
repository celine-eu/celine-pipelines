# REC Flexibility Commitments Pipeline

## Overview

The **REC Flexibility Commitments pipeline** mirrors flexibility commitment data from the **CELINE Flexibility API** into a PostgreSQL raw table.

It maintains a **90-day sliding window** of all commitment statuses (accepted, settled, rejected, cancelled), refreshed **every 15 minutes**.

---

## Data sources

- CELINE Flexibility API (internal, OIDC-authenticated)

License: Proprietary.

---

## Output datasets

- **RAW**
  - `flexibility_commitments_mirror` — full upsert mirror with status evolution tracking

No dbt transformation layers are included in this pipeline. The raw table serves as a source for downstream dbt pipelines computing settlement analytics, gamification, and acceptance rate metrics.

---

## Execution & Docker image

Docker image:
```
ghcr.io/celine-eu/pipeline-rec-flexibility-commitments
```

Run locally:
```bash
task pipeline:rec_flexibility_commitments:run
```

---

## Configuration & overrides

Schedule: every 15 minutes (`*/15 * * * *`)

Customizable options:
- Flexibility API URL (`CELINE_FLEXIBILITY_API_URL`)
- OIDC credentials (`CELINE_OIDC_CLIENT_ID`, `CELINE_OIDC_CLIENT_SECRET`)
- Retention window (default: 90 days)

See:
- `flows/config.yaml`

---

## Contributing

Contributions may include:
- additional commitment fields or metrics
- transformation layers (dbt silver/gold)
- improved pruning or deduplication logic

Ensure:
- access restrictions are respected (internal, contract-required)
- derived datasets are documented in governance
