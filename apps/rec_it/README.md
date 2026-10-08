# rec_it

Italian CER-specific settlement pipeline. Computes virtual self-consumption allocation per device under Italian GSE rules, and maintains the primary substation reference layer.

## Sources

| Table | Schema | Origin | Description |
|-------|--------|--------|-------------|
| `meters_data_15m` | `ds_dev_gold` | rec_metering pipeline | 15-min metered readings |
| `rec_device_membership` | `ds_dev_gold` | **`rec_registry` pipeline, in this repository** | Membership by device: `community_id`, `device_id`, `role`, `member_type`, `substation_id`, one row per `(community_id, device_id)` |
| `rec_registry_mirror` | `raw` | **`rec_registry` pipeline, in this repository** | REC participant registry, active members only, read here only by `rec_member_supply_points`: `user_id`, `community_id`, `area`, `role`, `member_type`, `topology_ids[]`, `delivery_point_ids[]` (points in service only), `sensor_ids[]`, `boundary_id`, `last_updated` |
| `gse_cabine_primarie` | `raw` | meltano (self-contained) | GSE primary substation open dataset |

**Schema resolution:** `ds_dev_gold` is read from the `CELINE_GOLD_SCHEMA` env var. Set this in `.env` to match your deployment. The `raw` schema is fixed.

**Providing private upstream tables:**

- **`meters_data_15m`** — produced by the `rec_metering` pipeline in this repository. Run rec_metering first.
- **`rec_device_membership`** and **`rec_registry_mirror`** — produced by the **`rec_registry` app in this repository**, which syncs the registry API into `raw.rec_registry_mirror` every 5 minutes and builds the membership view on it. Run that app first. Only if you cannot reach the registry API, create the mirror manually (DDL in `apps/rec_registry/flows/pipeline.py`), load sample data and run `dbt build` in `apps/rec_registry`.
- **`gse_cabine_primarie`** — self-contained: loaded via the included meltano extractor (`tap-copertura-cabine-primarie-gse`). Run `meltano run import` inside the `meltano/` directory.

The full source contracts are declared in `dbt/models/silver/sources.yml` and `dbt/models/gold/sources.yml`.

## dbt models

### Silver

#### Membership

`rec_it` keeps no copy of the registry: it reads `rec_device_membership` (rec_registry), whose `substation_id` is `topology_ids[1]` of the member's area. **Substation attribution rests on one invariant: an area lists exactly one topology node, a `primary_substation` whose id is the `cod_ac` of that area's GSE primary-substation boundary.** The registry enforces it from schema v0.7, and `rec_registry`'s singular test `rec_registry_mirror_substation_is_area_boundary` fails its run on an area breaking it (see `apps/rec_registry/README.md`).

A reading counts only when its `(device_id, community_id)` pair is a membership row. A device listed under two communities contributes each reading to the community the reading carries (its upstream topic), once.

#### `silver_gse_cabine_primarie`

Casts the raw JSON feature bag from the GSE open dataset into typed columns. Filters deleted records (`_sdc_deleted_at is null`). Outputs: `fid`, `geometry`, `cod_ac`, `rag_soc`, `cod_conseg`, `shape__area`, `shape__length`, `last_updated_at`.

### Gold

#### `rec_virtual_consumption_15m`

Community-level virtual self-consumption per `(ts, community_id, substation_id)`. Only readings of member devices of their community (`rec_device_membership`) contribute. Production is attributed only to `role='prosumer'` devices; a device of a `consumer` or `producer` member contributes consumption only.

```
self_consumption_kw = least(total_consumption_kw, total_production_kw)
self_consumption_ratio = self_consumption_kw / total_production_kw
```

Incremental merge on `(ts, community_id, substation_id)`.

#### `rec_virtual_consumption_hourly`

Hourly rollup of `rec_virtual_consumption_15m` via `date_trunc('hour', ts)`. Recomputes `self_consumption_ratio` from aggregated totals. Incremental merge on `(ts, community_id, substation_id)`.

#### `rec_virtual_consumption_per_device_15m`

Allocates the community's available self-consumption energy (`available_kwh = self_consumption_kw × 0.25`) to each device proportionally by consumption share:

```
ratio = device_consumption_kwh / total_consumption_kwh
virtual_consumption_kwh = ratio × available_kwh
```

Joins `meters_data_15m` with `rec_device_membership` and `rec_virtual_consumption_15m`. Incremental merge on `_id = md5(device_id || ts || substation_id)`: the community is left out of the key, because a reading reaches one community and a slug change then needs no re-keying.

#### `rec_virtual_consumption_per_device_hourly`

Hourly rollup of `rec_virtual_consumption_per_device_15m`. Incremental merge on `md5(device_id || ts_hour || substation_id)`.

#### Membership changes and history

The four `rec_virtual_consumption_*` models are incremental from their own `max(ts)` minus a short lookback (1 hour for the 15-minute models, 2 hours for the hourly roll-ups). The 15-minute models inner-join the current membership (`rec_device_membership`) on `(device_id, community_id)`; the hourly roll-ups read them. So a change in the registry reaches only the rows computed after it:

- **A meter attached late** has no rows for the intervals before it was attached, and the community totals for those intervals do not include it. A **meter detached** keeps its old rows.
- **A changed role or area** applies from the next run onwards; older rows keep the old role (whether production counted) and the old substation.
- **Per-device keys include `substation_id`**, so after an area change a device's older rows stay under the old substation and its new rows land under the new one; the two sets are never merged.

Bringing history in line with a changed membership needs a bounded recompute of these four models over the affected period, run by an operator. A `--full-refresh` recomputes all history against the registry as it is at that moment: a sensor that no active member holds by then drops out of the whole history, past included.

#### `rec_member_supply_points`

One row per `(user_id, community_id, pod_code)` — every active member's supply points in service, unnested from `rec_registry_mirror.delivery_point_ids` (the mirror leaves out a point flagged `active: false`, so a retired POD never reaches the distributor's list). A view, like `rec_device_membership`, so it always reflects the current mirror.

**Consent-gated per member, and not filtered here.** Governance declares `consent_required: true` with a `direct_user_match` row filter on `user_id`: read through a dataspace, the data plane narrows it to the members who consented to the sharing offer the query runs under — the list a community hands the party that offer names. No purpose is declared in this repository; the deployment binds the offer and its purpose.

#### `gse_cabine_primarie`

Promotes `silver_gse_cabine_primarie` to the gold schema. Incremental merge on `cod_ac` (updates geometry and attributes when the source dataset changes).

**This table is a runtime contract, not only a reference layer.** The Digital Twin resolves community areas against it through dataset-api: a point to the `cod_ac` whose shape contains it, and a `cod_ac` to its shape. Its readers depend on:

- `cod_ac` — the substation code, one row per code; community areas and topology nodes reference substations by it;
- `geometry` — PostGIS geometry in EPSG:4326 with a GIST index.

Renaming or retyping either column, changing the SRID or dropping the index breaks area resolution for onboarding and the community dashboard. **The merge never deletes:** a substation that disappears from a later GSE snapshot keeps its last row here (the silver filter on `_sdc_deleted_at` only stops further updates), so a withdrawn `cod_ac` still resolves. Removing it is a manual operation.

## Flow (`flows/pipeline.py`)

`rec-it-flow` runs two tasks in sequence: **Transform Gold Layer** (`dbt run --select tag:rec_it`) followed by **Run dbt Tests** (`dbt test --select tag:rec_it`). Every `rec_it` model and view carries the `rec_it` tag, so a view whose columns change (`rec_measurements_*`, `rec_member_supply_points`) is recreated by the next scheduled run, not left on its old column list. Serves with cron `*/15 * * * *` (every 15 minutes) in dev mode.
