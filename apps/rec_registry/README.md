# REC Registry Pipeline

## Overview

The **REC Registry pipeline** mirrors community membership data from the **CELINE REC Registry API** into a PostgreSQL raw table, then builds `rec_device_membership`, the one model every other REC app reads membership from.

It performs a **full-replace refresh every 5 minutes** from the registry's **member export** (`GET /admin/export/members`, rec-registry 1.7.0, REQ-0100), providing a stable source of every member with its status, grid area, topology nodes and meter sensors. The export names no member: no user id, name, e-mail, DID, address, extras or supply points (POD codes), so the mirror holds none. An export with no member empties the table; a suspended member is mirrored as suspended, and `rec_device_membership` keeps active members only.

---

## Data sources

- CELINE REC Registry API (internal, OIDC-authenticated): `GET /admin/export/members`, JSON, under the `export` action (`rec-registry.export`), every community. Read with a direct HTTP call carrying the bearer token of the same OIDC client-credentials provider as before; the full YAML export (`/admin/export`) is no longer read.

License: Proprietary.

---

## Output datasets

- **RAW**
  - `rec_registry_mirror` — full-replace mirror of every community member, whatever its status (one row per member/community pair). An export with no member empties it. Columns: `member_key` (the member's registry key), `community_id` (the export's community key, the community's slug, carried unchanged), `area` (the member's area key), `role`, `member_type`, `status` (`active`, `suspended`, ...: only `active` rows reach `rec_device_membership` and the substation test), `topology_ids` (text[], the node ids listed by the member's area), `sensor_ids` (text[], the sensor ids of the member's meters), `boundary_id` (the id of the member's area's boundary, registry schema v0.7: the `cod_ac` of its GSE primary substation; null when the area has no boundary), `last_updated`. Primary key `(member_key, community_id)`. It was keyed by `(user_id, community_id)` and held active members only until the member export: the flow renames `user_id` to `member_key` in an existing table (keeping its primary key) and adds `status` with `active` for the rows already there (they were all active), then the same run replaces the rows. An older image cannot write to the migrated table (no `user_id` column): rolling back means renaming the column back. The column was named `rec_id` before; the table is created only when missing and never altered, so a deployment upgrading from it drops the table once and the next run recreates it. `boundary_id` was added after the table was first deployed; the flow adds it to an existing table (`ADD COLUMN IF NOT EXISTS`) on its next run, and rows keep a null value until the registry exports a boundary for their area. `delivery_point_ids` was removed the same way: the flow drops it from an existing table (`DROP COLUMN IF EXISTS`). **Drop the retired view first** (`DROP VIEW IF EXISTS <gold schema>.rec_member_supply_points`): dbt does not remove a view whose model was deleted, and Postgres refuses to drop a column a view still reads, so the flow fails until the view is gone.

Every community the registry exports lands in the same table, keyed by `community_id`. A sensor listed on two active members appears in two rows of the view, and downstream models would count it twice: the `rec_device_membership_unique_per_community` test fails the run on it (below). The registry refuses that case at the source (`409 sensor_held`: one active holder per sensor id across every community, from rec-registry 1.6.0); a pair stored before then stays until it is fixed, and the registry's `duplicate-sensors` command lists them.

## Substation attribution check

`rec_it` takes `topology_ids[1]` as a member's `substation_id`. That is right only when the member's area lists exactly one topology node and its id is the area's `boundary_id`. On every run, before the mirror is replaced, the **Check substation attribution** task flags each `(community_id, area)` whose active members' rows break this: it logs one warning per area (community id and area key only, never a member) and reports `areas_mismatched` in its result. It does **not** refuse the export: withholding it would leave the previous mirror feeding `rec_it`, which is no better. Rows with a null `boundary_id`, and rows of inactive members, are not checked.

The registry refuses such areas on area writes, topology node writes and bundle import from schema v0.7, so a hit means an area stored before that (the registry's `invalid-area-boundaries` command lists them). The same invariant is a dbt singular test of this app (`rec_registry_mirror_substation_is_area_boundary`), which fails the flow's dbt build.

---

## `rec_device_membership` (gold, dbt)

A view over the mirror's `status = 'active'` rows: one row per `(community_id, device_id)` for every meter sensor of every active member, with `role`, `member_type` and `substation_id` (`topology_ids[1]`). No `user_id`. It is the population of `rec_it` and the fleet of `rec_flexibility` (every role), both joined on `(device_id, community_id)`. A device the registry lists under two communities appears under each; which community a *reading* belongs to is the reading's own (its upstream topic), so no reading is copied into the second.

The flow runs `dbt build` right after the mirror is replaced. `rec_device_membership_unique_per_community` fails it when a sensor is listed twice in one community: the listing is fixed in the registry, never deduplicated downstream. Governance declares the view with the `rec_registry` row filter on `device_id`: a person reading it with their own token sees only their own devices.

---

## Known limitations

- **Only the mirror is current.** Downstream incremental models recompute a short trailing window, so a membership change (a meter attached or detached, a role or area changed) reaches rows from the next run onwards; older rows keep the values they were computed with. See `apps/rec_it/README.md`.

---

## Tests

Pure-Python tests of the member-export request (mock HTTP transport), the flatten (every member with its status, no identity or supply point mirrored), the table replace and the substation check, on synthetic exports (no database, no registry). One test applies the DDL to an old-shaped table on a real Postgres; it runs only when `REC_REGISTRY_TEST_PG_DSN` names a throwaway database:

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
