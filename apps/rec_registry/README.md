# REC Registry Pipeline

## Overview

The **REC Registry pipeline** mirrors community membership data from the **CELINE REC Registry API** into a PostgreSQL raw table, then builds `rec_device_membership`, the one model every other REC app reads membership from.

It performs a **full-replace refresh every 5 minutes**, providing a stable source of active members, their grid areas, topology nodes, delivery points, and meter sensors. An export with no active member empties the table, so a community whose last active member is suspended leaves no row behind.

---

## Data sources

- CELINE REC Registry API (internal, OIDC-authenticated)

License: Proprietary.

---

## Output datasets

- **RAW**
  - `rec_registry_mirror` — full-replace mirror of active community members (one row per user/community pair). An export with no active member empties it: the last active member of a community suspended leaves no row behind. Columns: `user_id`, `community_id` (the bundle's `community.id`, the community's slug, carried unchanged), `area` (the member's area key), `role`, `member_type`, `topology_ids` (text[], the node ids listed by the member's area), `delivery_point_ids` (text[], the member's delivery points in service: a point flagged `active: false` in the registry is left out, an unflagged one counts as active), `sensor_ids` (text[], the `sensor_id` of each meter asset), `boundary_id` (the id of the member's area's boundary, `area.boundary.id` in registry schema v0.7: the `cod_ac` of its GSE primary substation; null when the area has no boundary), `last_updated`. Primary key `(user_id, community_id)`. The column was named `rec_id` before; the table is created only when missing and never altered, so a deployment upgrading from it drops the table once and the next run recreates it. `boundary_id` was added after the table was first deployed; the flow adds it to an existing table (`ADD COLUMN IF NOT EXISTS`) on its next run, and rows keep a null value until the registry exports a boundary for their area.

Every community the registry exports lands in the same table, keyed by `community_id`. A sensor listed on two active members appears in two rows, and downstream models would count it twice: the `rec_device_membership_unique_per_community` test fails the run on it (below). The registry refuses that case at the source (`409 sensor_held`: one active holder per sensor id across every community, from rec-registry 1.6.0); a pair stored before then stays until it is fixed, and the registry's `duplicate-sensors` command lists them.

## Substation attribution check

`rec_it` takes `topology_ids[1]` as a member's `substation_id`. That is right only when the member's area lists exactly one topology node and its id is the area's `boundary_id`. On every run, before the mirror is replaced, the **Check substation attribution** task flags each `(community_id, area)` whose rows break this: it logs one warning per area (community id and area key only, never a member) and reports `areas_mismatched` in its result. It does **not** refuse the export: withholding it would leave the previous mirror feeding `rec_it`, which is no better. Rows with a null `boundary_id` are not checked.

The registry refuses such areas on area writes, topology node writes and bundle import from schema v0.7, so a hit means an area stored before that (the registry's `invalid-area-boundaries` command lists them). The same invariant is a dbt singular test of this app (`rec_registry_mirror_substation_is_area_boundary`), which fails the flow's dbt build.

---

## `rec_device_membership` (gold, dbt)

A view over the mirror: one row per `(community_id, device_id)` for every meter sensor of every active member, with `role`, `member_type` and `substation_id` (`topology_ids[1]`). No `user_id`. It is the population of `rec_it` and the fleet of `rec_flexibility` (every role), both joined on `(device_id, community_id)`. A device the registry lists under two communities appears under each; which community a *reading* belongs to is the reading's own (its upstream topic), so no reading is copied into the second.

The flow runs `dbt build` right after the mirror is replaced. `rec_device_membership_unique_per_community` fails it when a sensor is listed twice in one community: the listing is fixed in the registry, never deduplicated downstream. Governance declares the view with the `rec_registry` row filter on `device_id`: a person reading it with their own token sees only their own devices.

---

## Known limitations

- **Only the mirror is current.** Downstream incremental models recompute a short trailing window, so a membership change (a meter attached or detached, a role or area changed) reaches rows from the next run onwards; older rows keep the values they were computed with. See `apps/rec_it/README.md`.

---

## Tests

Pure-Python tests of the flatten (inactive members and inactive delivery points left out), the table replace and the substation check, on synthetic bundles (no database, no registry):

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
