# REC Registry Pipeline

## Overview

The **REC Registry pipeline** mirrors community membership data from the **CELINE REC Registry API** into a PostgreSQL raw table.

It performs a **full-replace refresh every 5 minutes**, providing a stable source of active members, their grid areas, topology nodes, delivery points, and meter sensors. One exception: when the export holds no active member at all, the run writes nothing and the previous rows stay (see [Known limitations](#known-limitations)).

---

## Data sources

- CELINE REC Registry API (internal, OIDC-authenticated)

License: Proprietary.

---

## Output datasets

- **RAW**
  - `rec_registry_mirror` — full-replace mirror of active community members (one row per user/community pair), except after an empty export (see [Known limitations](#known-limitations)). Columns: `user_id`, `rec_id`, `area` (the member's area key), `role`, `member_type`, `topology_ids` (text[], the node ids listed by the member's area), `delivery_point_ids` (text[]), `sensor_ids` (text[], the `sensor_id` of each meter asset), `boundary_id` (the id of the member's area's boundary, `area.boundary.id` in registry schema v0.7: the `cod_ac` of its GSE primary substation; null when the area has no boundary), `last_updated`. Primary key `(user_id, rec_id)`. `boundary_id` was added after the table was first deployed; the flow adds it to an existing table (`ADD COLUMN IF NOT EXISTS`) on its next run, and rows keep a null value until the registry exports a boundary for their area.

Every community the registry exports lands in the same table, keyed by `rec_id`. A sensor listed on two active members appears in two rows, and downstream models count it twice. The registry refuses that case at the source (`409 sensor_held`: one active holder per sensor id across every community, from rec-registry 1.6.0); a pair stored before then stays until it is fixed, and the registry's `duplicate-sensors` command lists them.

## Substation attribution check

`rec_it` takes `topology_ids[1]` as a member's `substation_id`. That is right only when the member's area lists exactly one topology node and its id is the area's `boundary_id`. On every run, before the mirror is replaced, the **Check substation attribution** task flags each `(rec_id, area)` whose rows break this: it logs one warning per area (community id and area key only, never a member) and reports `areas_mismatched` in its result. It does **not** refuse the export: withholding it would leave the previous mirror feeding `rec_it`, which is no better. Rows with a null `boundary_id` are not checked.

The registry refuses such areas on area writes, topology node writes and bundle import from schema v0.7, so a hit means an area stored before that (the registry's `invalid-area-boundaries` command lists them). The same invariant is a dbt singular test in `rec_it` (`rec_registry_mirror_substation_is_area_boundary`), which fails the `rec_it` test stage.

---

No dbt transformation layers are included in this pipeline. The raw table serves as a source for downstream dbt pipelines computing virtual self-consumption, billing, and community analytics.

---

## Known limitations

- **An empty export leaves the previous mirror in place.** `mirror_to_db` returns before the `TRUNCATE` when the export has no active member rows, so the rows of the last non-empty export keep feeding the downstream dbt models. This happens when every member of every community is suspended, removed or deleted, or when a community is retired before any new member is active. Until the flow truncates on an empty export, clear the table by hand in that situation (`TRUNCATE raw.rec_registry_mirror`). Issue: [#7](https://github.com/celine-eu/celine-pipelines/issues/7).
- **Only the mirror is current.** Downstream incremental models recompute a short trailing window, so a membership change (a meter attached or detached, a role or area changed) reaches rows from the next run onwards; older rows keep the values they were computed with. See `apps/rec_it/README.md`.

---

## Tests

Pure-Python tests of the flatten and the substation check, on synthetic bundles (no database, no registry):

```bash
uv run pytest apps/rec_registry/tests -q
```

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
