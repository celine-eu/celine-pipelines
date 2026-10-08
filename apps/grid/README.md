# grid

Computes daily wind and heat risk overlays for the distribution grid network, combining CIM-normalized grid topology with Open-Meteo weather forecasts.

## Upstream dependency

Reads from two CIM-normalized silver tables produced by a DSO-specific ingestion pipeline (not included in this repository). The expected schema is declared in `dbt/models/sources.yml` — this is the contract between the private upstream and this pipeline.

**Schema resolution:** the source schema is read from the `CELINE_SILVER_SCHEMA` env var (default: `ds_dev_silver`). Weather sources use `CELINE_GOLD_SCHEMA` (default: `ds_dev_gold`). Set these in `.env` to match your deployment.

**Providing the tables:** the upstream pipeline must materialise the tables below into the configured schema before this pipeline runs. If you are adapting this pipeline for a new DSO, create your own ingestion pipeline that normalises the DSO's raw CIM export into these two silver tables. Alternatively, for local development, you can create the tables manually with DDL matching the columns below and load sample data.

**`ds_dev_silver.silver_grid_ac_line_segment`** — MT line segments with the following expected columns:

| Column | Description |
|--------|-------------|
| `dso_id` | Organisation alias of the DSO (its Keycloak organisation, e.g. `example-dso`), stamped on every row in the upstream ingestion pipeline |
| `line_name` | ACLineSegment name |
| `conductor_type` | `overhead_bare` \| `overhead_insulated` \| `underground_cable` |
| `parent_substation_name` | Upstream HV/MV substation |
| `operational_unit` | Distribution operational unit |
| `feeder_id` | Feeder circuit identifier |
| `municipality` | Administrative municipality |
| `length_m` | Segment length (m) |
| `is_vegetated_zone` | True when segment passes through a forested area |
| `elevation_start_m` | Elevation at start (vegetated zones only) |
| `elevation_end_m` | Elevation at end (vegetated zones only) |
| `thermal_tier` | `low` \| `mid` \| `high` \| NULL: thermal exposure of the arc (NULL where the thermal model has no coverage) |
| `thermal_margin_c` | Margin (°C) between the peak conductor temperature and the rating of its insulation |
| `thermal_theta_max_c` | Maximum admissible conductor temperature (°C) of the insulation class |
| `thermal_insulation` | Insulation class of the arc |
| `thermal_scadacontr` | SCADA contract identifier the thermal model keyed on |
| `geom` | Geometry in EPSG:32632 (UTM zone 32N) |

**`ds_dev_silver.silver_grid_geo_thermal_joints`**: MT cable joints (giunti) of the thermal model:

| Column | Description |
|--------|-------------|
| `dso_id` | Organisation alias of the DSO (its Keycloak organisation, e.g. `example-dso`), stamped on every row in the upstream ingestion pipeline |
| `joint_id` | Joint identifier (unique per DSO) |
| `comune` | Administrative municipality |
| `insulation_class`, `theta_max_c` | Insulation class and its maximum admissible temperature (°C) |
| `technology` | Joint technology (`RESINA`, `TERMORESTRINGENTE`, `AUTORESTRINGENTE`, ...) |
| `anno_posa`, `eta_anni` | Installation year and age |
| `is_asphalt` | True when buried under asphalt (worse heat dissipation) |
| `theta_peak_c`, `margin_min_c`, `m_r_critico` | Peak temperature, minimum margin and critical resistance ratio of the physical model |
| `joint_tier` | `low` \| `mid` \| `high` \| NULL: thermal exposure of the joint |
| `geom` | Point geometry in EPSG:32632 |

`ds_dev_silver.silver_grid_geo_thermal_cables` (the per-arc detail behind the `thermal_*` columns above) is declared as a source for completeness; no model reads it.

**`ds_dev_silver.silver_grid_substation`** — MV/LV substations (`voltage_class='mv_lv'`) with the following expected columns:

| Column | Description |
|--------|-------------|
| `dso_id` | Organisation alias of the DSO (its Keycloak organisation, e.g. `example-dso`), stamped on every row in the upstream ingestion pipeline |
| `asset_id` | IdentifiedObject.mRID (unique) |
| `name` | Substation name |
| `label_id`, `label` | Display identifiers |
| `line_name`, `feeder_id` | Associated circuit |
| `parent_substation_name` | Upstream HV/MV substation |
| `operational_unit` | Distribution operational unit |
| `municipality` | Administrative municipality |
| `geom` | Geometry in EPSG:32632 |

## dbt models

Every exposed gold model carries `dso_id`, the operator's organisation alias inherited from the silver source, and declares the row filter `organization_match {org_type: dso, column: dso_id}`: a reading group of a `dso` organisation reads only the rows whose `dso_id` is that organisation's alias. Every figure computed across assets (the trendline, the tile grid) is computed per operator.

`dso_id` is part of every asset id (`segment_id`, `span_id`) and of the unique keys of the incremental tables. Changing its value on existing data is a migration (re-key the ids and the history in place), never a full refresh: the incremental tables keep days the weather sources no longer serve.

### `grid_wind_risks`

Wind risk per overhead MT line segment (all conductor types except `underground_cable`). Spatially joins segments with `om_wind_gusts` (Open-Meteo): nearest weather station within 5 km per segment per date. `DISTINCT ON (date, line_name, municipality, conductor_type, length_m)` keeps the closest station. Covers today + 2 days ahead.

`risk_level` = `ALERT | WARNING | NORMAL` (mapped from `gust_excess_tier`). `feature_geojson` is a ready-to-render GeoJSON Feature with stroke styling and risk metadata in `properties`.

Materialized as **incremental** (`schema: gold`) — historical rows are preserved across runs. Each daily run upserts today + 2 days; unique key on `(date, dso_id, line_name, municipality, conductor_type, length_m)`.

### The heat vector: soil temperature x thermal tier

Air temperature alone fires on every hot afternoon and says nothing about the ground a cable is buried in. The heat vector is therefore a matrix of two axes: how warm the *ground* is at the location, and how little *thermal margin* the asset has.

**Weather axis** (`grid_heat_status`, `daily`, table). One row per Open-Meteo point per forecast date, `heat_status`:

| | air GREEN / ORANGE | air RED |
|---|---|---|
| **soil GREEN** | GREEN | GREEN |
| **soil ORANGE** | ORANGE | **RED** |

The air axis is `om_heat_risk` (Tmax over the altitude-band P90 with a heat-day streak); the soil axis is `om_soil_heat_risk` (1 m soil temperature against its own 7-day P90). The two tables do **not** share coordinates: `om_heat_risk` stores the lat/lon the API snapped the request to, `om_soil_heat_risk` the requested grid point, so they are matched by **nearest geoposition within 10 km**, never by equality. The soil row is taken as-of (latest complete row on or before the date, up to 10 days back) and the staleness is exposed as `soil_asof_date`. No soil row in reach reads GREEN: per the matrix below, GREEN status maps to NORMAL for every tier, so a soil-feed outage switches the whole heat vector off rather than falling back to an air-only assessment. `grid_heat_status_has_soil` is an **error**-severity test for exactly this reason: because GREEN is the default and silently turns risk detection off, soil coverage is load-bearing, not merely informative, so a coverage gap must fail the build rather than warn. Om's `om_soil_is_fresh` (warn, in the om pipeline) is the earlier signal for the same underlying problem (stale/missing soil data).

**Asset axis**: `thermal_tier`, the margin between the peak conductor temperature of the asset and the rating of its insulation. Cables carry it on `silver_grid_ac_line_segment`, joints as `joint_tier`.

**The matrix** (macro `grid_heat_matrix`, the single place it is written):

| `heat_status` | tier `low` / `mid` | tier `high` |
|---|---|---|
| GREEN | NORMAL | NORMAL |
| ORANGE | NORMAL | WARNING |
| RED | WARNING | ALERT |

A NULL status (no weather point within reach) yields a NULL level: the asset is not assessed, not "safe". `escalated_by_thermal` (macro `grid_heat_escalated`) marks the rows the thermal axis lifted: tier `high` under an ORANGE or RED status, the heat counterpart of `escalated_by_tree_strike`.

### `grid_heat_risks`

Heat risk per underground cable segment (`conductor_type = 'underground_cable'`), against `grid_heat_status` (nearest point within 5 km per segment per date, today + 2 days). Heat risk applies to buried conductors sensitive to soil temperature; overhead lines are excluded.

Arcs the thermal model does not cover read `thermal_tier = 'low'` with `thermal_modelled = false`, so a coverage gap never invents a high-tier risk. This is not the same as the old air-only model, though: under the matrix an ORANGE status now reads NORMAL for tier `low`/`mid`, where the pre-matrix air-only model read WARNING. This province-wide reduction in sensitivity for uncovered/low-tier arcs is intended by the tier-based matrix decision, not a regression.

Materialized as **incremental** (`schema: gold`) with `on_schema_change='append_new_columns'`, the same accumulation strategy as `grid_wind_risks`. The columns added by the two-axis model reach an existing table on the next run with NULLs; a one-off `--full-refresh` is required to populate them, but note it does **not** recompute history by itself, since `date_range` defaults to today + 2 days (see "Upgrading to the two-axis heat vector (0.13.0)" below for what a plain `--full-refresh` actually does and how to recompute history on purpose).

### `grid_joint_heat_risks`

Heat risk per MT cable joint. A joint is the weak point of a buried run: the same soil, a shorter thermal path and an insulation class of its own, so the physical model tiers it separately from the arcs around it. Same matrix, on `joint_tier`, same 5 km nearest-point join.

Only joints the thermal model tiered are assessed (`joint_tier is not null`): an untiered joint would read `low` and pretend to be safe. Those joints still appear on the map with `thermal_tier = 'unmodelled'`, they simply carry no risk row.

A joint has no length, so it is deliberately **absent from `grid_risk_km`**, and therefore from the DSO alert e-mail, which reads that table. Joint risk is a map and detail-panel signal: `grid_risks` (`metrics -> 'asset_type' = 'joint'`), `grid_risks_8h`, `grid_risks_now`.

### `grid_substations`

Static GeoJSON map layer for MV/LV substations. No weather join. Projects `silver_grid_substation` to gold with WGS84 `longitude`/`latitude` columns and a `feature_geojson` point feature for map rendering.

Materialized as table (`schema: gold`). Updated on the monthly topology cadence.

### `grid_network_topology`

Distinct topology entities (lines, substations, operational units, municipalities, conductor types) sourced directly from `silver_grid_ac_line_segment`. Powers the `/filters` UI endpoint — ensures all filter options are present even for segments with no nearby weather station. Updated on the monthly topology cadence.

Materialized as table (`schema: gold`).

### `grid_risk_km`

Length-weighted risk exposure per tratta (`line_name × municipality × conductor_type`, the `grid_shapes` grain and the same `segment_id`), split by `operational_unit`, per forecast date and risk vector. Aggregated from `grid_wind_risks` / `grid_heat_risks` (the only models carrying `length_m`): `km_total`, `km_alert`, `km_warning`, `km_normal`, `km_escalated`, the static `km_tree_high` / `km_tree_mid` (wind) and `km_thermal_high` / `km_thermal_mid` (heat), `worst_level`, `metric_max` and `risk_index = 100·(km_alert + 0.5·km_warning)/km_total`. On heat rows `km_escalated` is the thermal escalation. Cable joints are excluded, having no length: the alert dispatcher that reads this table does not see them. The denominator is the length of the fragments with a weather match, so the conductor scope is implicit (wind → overhead, heat → underground). Rollups per line and per operational unit are exact sums of these rows and are computed by the DT `risk_km` value fetcher.

Materialized as **incremental** (`schema: gold`); each daily run rewrites today + future rows (pre-hook delete), history is preserved. Singular tests in `dbt/tests/grid_risk_km_*.sql` check that the km parts sum to the total, that the index stays in `[0, 100]` and that the aggregate matches the fragment km per date and vector.

### `grid_wind_risks_8h` and `grid_risks_8h`

The intra-day view. `grid_wind_risks_8h` is `grid_wind_risks` joined to `om_wind_gusts_8h` (the same gust model on 8-hour windows, 00–08 / 08–16 / 16–24) — same nearest-station join, tiers, tree-strike escalation, `unique key` extended with `window_start`. `grid_risks_8h` collapses it like `grid_risks` (WARNING/ALERT per `segment_id`, `metrics` JSONB) and adds `window_start` / `slot`; heat rows are the daily `grid_heat_risks` rows repeated on the three windows of their day, since the heat forecast is a daily max. Both are `daily`, incremental; today + 2 days are rewritten on each run. The daily `om_wind_gusts` is the worst of the day's windows, so the daily map is the union of the three windows by construction.

### `grid_tile_grid`, `grid_tree_strike_spans`, `grid_tree_strike_tiles`

`grid_tile_grid` is the 5 km × 5 km UTM tile grid, one per operator over the extent of its network (`tile_id` is unique per `dso_id`), shared by `grid_tiles` (segments) and `grid_tree_strike_tiles` (tree-strike spans), so both layers are loaded per viewport with the same tile ids.

`grid_tree_strike_spans` publishes the spans of the tree-strike LiDAR analysis (`silver_grid_geo_tree_strike`, ~3.6k overhead spans of ~100 m) as their own static overlay, with geometry, `tier`, `multiplier`, `strike_density_km`, `n_strike`, `length_m`, the `dso_id` the silver row is stamped with, and the `operational_unit` / feeder / primary substation inherited from the nearest CIM segment of the same line within 50 m. The operator never comes from geometry: a nearest segment of another operator fails the test `grid_tree_strike_spans_dso_matches_nearest_segment`. `span_id` is minted from operator, line, municipality, conductor type and a geometry hash (the export's positional `seg_id` renumbers on every regeneration). This is the exposure layer only: the wind escalation on the tratte (`grid_wind_risks`) is unchanged.

All three are `monthly` topology tables (rebuilt with `grid_shapes`).

### `grid_shapes` and the joint layer

`grid_shapes` is the unified asset registry: `ac_line_segment`, `substation` and, since 0.13.0, `joint`. Joint rows carry `segment_id = md5(dso_id || '|' || 'joint' || '|' || joint_id::text)` (the same expression the joint rows of `grid_risks` mint, byte for byte), `asset_key = joint_id`, `name = 'Giunto <joint_id>'`, `municipality = comune` and a point `feature_geojson`. Every row also carries `thermal_tier`, `thermal_margin_c`, `thermal_theta_max_c`, `thermal_insulation` and, on joints only, `is_asphalt`, `anno_posa`, `technology`, `m_r_critico`.

On a line the tier is the worst of its arcs (a tratta is as exposed as its hottest cable), the margin and rating are the minimum, and the insulation reported is that of the tightest-margin arc. `unmodelled` is a joint-only value; lines outside the thermal model leave `thermal_tier` NULL.

`grid_tiles` and `grid_tile_index` need no change: `ST_Intersects` places a point in its tile like any other geometry, so joints are loaded per viewport with the segments.

The tree-strike columns of a line follow the same worst-of rule, because the worst fragment is what escalates the wind risk: `strike_tree_tier` is the worst tier among the fragments of the tratta. Since a tratta is the union of many fragments (ALBIANO/CIVEZZANO overhead bare is 6 km made of ~25 of them), that single word overstates the exposure, so the row also carries `strike_km_high` / `strike_km_mid` / `strike_km_low` (km of fragments per tier) and `strike_density_per_km` is the length-weighted density over the whole tratta (unforested fragments count as zero), not the density of the worst fragment. Both are NULL where no fragment is tiered. The map popup shows the tier of the span under the cursor next to this breakdown.

### Macros

- `grid_risk_color(tier)` — maps `ALERT | WARNING | NORMAL` to a hex colour for map rendering.
- `grid_geojson_feature(geom_col, ...)` — builds a GeoJSON Feature string with risk-aware stroke styling from a geometry column.
- `grid_heat_matrix(heat_status, thermal_tier)`: the soil x thermal risk matrix, the single place it is written.
- `grid_heat_escalated(heat_status, thermal_tier)`: true when the thermal axis is what lifted the level.

## Upgrading to the two-axis heat vector (0.13.0)

`grid_heat_risks` is incremental with `on_schema_change='append_new_columns'`, so the first run after
deploy adds the new columns to the existing table but leaves every historical row with NULL in them.
A **one-off `--full-refresh` is required, not optional**: until it is run, `not_null_grid_heat_risks_thermal_tier`,
`not_null_grid_heat_risks_escalated_by_thermal` and the load-bearing `grid_heat_risks_matrix_truth_table`
are red on the historical rows.

**`--full-refresh` alone does not recompute history.** Every model in this pipeline builds its rows from
a `date_range` CTE that defaults to today through today + 2 days unless a `start_date` var is passed. A
plain `dbt run --full-refresh` therefore drops the table and rebuilds only those three days: history before
the refresh is **discarded**, not recomputed. If you need the thermal columns populated for historical
dates, pass `--vars '{"start_date": "<earliest date>"}'` on every command below. Two limits on how far back
that can go:
- The soil axis (`om_soil_heat_risk`) only exists from om 0.33.0's first run, about 21 days back from
  today at the time of writing. Dates before that have no soil row to join, so they cannot be
  reconstructed with the two-axis matrix at all (see "Historical backfill" below for the GREEN-default
  behaviour this produces).
- If the pre-upgrade history in `grid_heat_risks` / `grid_risks` / `grid_risk_km` matters for anything
  (audits, trend charts, the DSO alert log), **snapshot those tables before running `--full-refresh`**,
  as a plain snapshot/export, since this pipeline does not manage history any other way.

```bash
dbt run --select grid_heat_status grid_joint_heat_risks grid_heat_risks --full-refresh
dbt run --select grid_risks grid_risks_8h grid_risk_km grid_risks_trendline --full-refresh
dbt run --select tag:monthly          # grid_shapes gains the joint rows, the thermal columns and strike_km_*
dbt test
```

`grid_shapes` must be rebuilt in the same window, or the joint rows of `grid_risks` point at a
`segment_id` the map does not know yet (the warn test `grid_joint_segment_ids_exist_in_shapes` says so).

**Dataset-api catalogue re-import.** After the dbt steps above, re-run the dataset-api catalogue import
so it picks up the new tables and columns (`grid_heat_risks.thermal_tier`, `grid_shapes` joint rows, and
the rest of the two-axis columns). This must happen before the digital-twin 1.13.0 release: DT selects
these new columns/tables directly, and dataset-api rejects the query until they are re-imported into its
catalogue.

**Deploy order for 0.13.0**: grid dbt steps above -> dataset-api catalogue re-import -> digital-twin 1.13.0
release. `grid 0.13.0` also **hard-depends on `om_soil_heat_risk` existing** as a source (it is read via
`source()`, not `ref()`, so dbt's DAG will not catch a missing table): om 0.33.0 must have run at least
once, populating `om_soil_heat_risk`, before the grid daily flow runs for the first time after this
upgrade.

## Historical backfill

To populate risk history, sync the upstream source tables first (`om_wind_gusts`, `om_heat_risk`,
**`om_soil_heat_risk`**, `silver_grid_ac_line_segment`, `silver_grid_geo_thermal_joints`), then run with
a `start_date` override:

```bash
dbt run --select grid_wind_risks grid_heat_status grid_heat_risks grid_joint_heat_risks \
        grid_risks grid_risks_8h grid_risk_km grid_risks_trendline \
        --vars '{"start_date": "2024-01-01"}'
```

`grid_heat_status` **must be in the selection**. It is the weather source of both heat models and it
honours the same `start_date` var; run the heat models without it and the status table still holds only
today + 2 days, so every backfilled row falls outside the join and silently gets a NULL date and a NULL
`heat_status`, which the matrix turns into a NULL `risk_level`.

The `date_range` CTE will expand from `start_date` through today + 2 days in all four models. The
incremental unique keys prevent duplicates on re-runs. Note that the soil axis reads GREEN wherever
`om_soil_heat_risk` has no complete row within 10 days of the backfilled date, and GREEN maps to NORMAL
for every tier under the matrix, so a backfill that predates the soil history silently produces an
all-NORMAL heat vector for those dates rather than failing or falling back to an air-only assessment.

## Flow (`flows/pipeline.py`)

`grid-resilience-flow` runs two tasks in sequence: **Transform Gold Layer** (`dbt run --select tag:daily`) followed by **Test grid models**. Schedule is configured in `flows/config.yaml`: cron `15 8 * * *` (daily at 08:15 UTC), after the Open-Meteo wind (08:00 run), heat **and soil** pipelines complete.

`flows/pipeline_nowcasting.py` runs `tag:nowcast` every 15 minutes.

### The monthly models are not in any flow

`tag:monthly` (`grid_shapes`, `grid_tile_grid`, `grid_tiles`, `grid_tile_index`, `grid_tree_strike_spans`, `grid_tree_strike_tiles`, `grid_substations`, `grid_network_topology`) is the topology cadence: it is **not** part of the daily flow and has no schedule of its own. Run it by hand whenever the silver topology changes, which includes any change to the thermal model (new joints, retiered arcs):

```bash
dbt run --select tag:monthly
dbt test --select tag:monthly
```

Until it is run, `grid_risks` can carry joint rows whose `segment_id` is not yet in `grid_shapes` and the map panel opens on nothing: the singular test `grid_joint_segment_ids_exist_in_shapes` warns (not fails) exactly for that case.

One caveat on the tiling: `grid_tile_grid` derives each operator's origin from the `ST_Extent` of that operator's `grid_shapes`, so a joint (or any asset) lying outside the current bounding box by more than the 5 km snap can shift the origin and renumber every `tile_id`. Clients cache tiles by id, so check `grid_tile_index` after a monthly build that changed the asset extent.
