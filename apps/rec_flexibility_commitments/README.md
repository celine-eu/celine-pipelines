# REC Flexibility Commitments Pipeline

## Overview

The **REC Flexibility Commitments pipeline** mirrors flexibility commitment data from the **CELINE Flexibility API** into a PostgreSQL raw table.

It maintains a **90-day sliding window** of all commitment statuses (accepted, settled, rejected, cancelled), refreshed **every 15 minutes**.

> **The mirror equals the export's window, the empty window included.** Each run reads
> `GET /api/commitments/export?created_after=<now - 90 days>`, then deletes every row and
> inserts the fetched ones in one transaction: a commitment the export no longer lists
> leaves the mirror, and rows older than the window are pruned with it. An export with no
> commitment empties the table. A fetch that fails raises before the mirror is touched, and
> the previous rows stay.

The same transaction upserts the run marker in `raw.mirror_refresh`
(`mirror = 'flexibility_commitments_mirror'`, `refreshed_at`, `rows`). `last_updated`
cannot tell a stopped flow from an empty export; the marker can. `rec_flexibility`'s test
`flexibility_commitments_mirror_is_fresh` fails when the marker is missing or older than
2 hours.

---

## Data sources

- CELINE Flexibility API (internal, OIDC-authenticated)

License: Proprietary.

---

## Output datasets

- **RAW**
  - `flexibility_commitments_mirror` — full replace of the export's 90-day window, current status per commitment
  - `mirror_refresh` — one run marker per raw mirror (this pipeline writes the row of `flexibility_commitments_mirror`)

No dbt transformation layers are included in this pipeline. The raw table serves as a source for downstream dbt pipelines computing settlement analytics, gamification, and acceptance rate metrics.

---

## Tests

```bash
uv run pytest apps/rec_flexibility_commitments/tests -q
```

A stub flexibility-api answers over real HTTP; no database. The tests in
`test_the_mirror_on_postgres.py` run only when `REC_FLEXIBILITY_COMMITMENTS_TEST_PG_DSN`
names a throwaway Postgres (they drop and recreate its `raw` schema).

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
- the same run marker for other raw mirrors

Ensure:
- access restrictions are respected (internal, contract-required)
- derived datasets are documented in governance
