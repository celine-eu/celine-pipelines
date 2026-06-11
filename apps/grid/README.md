# grid

Computes daily wind and heat risk overlays for the distribution grid network, combining CIM-normalized grid topology with Open-Meteo weather forecasts.

## Upstream dependency

Reads from two CIM-normalized silver tables produced by a DSO-specific ingestion pipeline (not included in this repository). The expected schema is declared in `dbt/models/sources.yml` — this is the contract between the private upstream and this pipeline.

**Schema resolution:** the source schema is read from the `CELINE_SILVER_SCHEMA` env var (default: `ds_dev_silver`). Weather sources use `CELINE_GOLD_SCHEMA` (default: `ds_dev_gold`). Set these in `.env` to match your deployment.

**Providing the tables:** the upstream pipeline must materialise the tables below into the configured schema before this pipeline runs. If you are adapting this pipeline for a new DSO, create your own ingestion pipeline that normalises the DSO's raw CIM export into these two silver tables. Alternatively, for local development, you can create the tables manually with DDL matching the columns below and load sample data.

**`ds_dev_silver.silver_grid_ac_line_segment`** — MT line segments with the following expected columns:

| Column | Description |
|--------|-------------|
| `dso_id` | UUID of the DSO — stamped from `raw.dso_registry` in the upstream ingestion pipeline |
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
| `geom` | Geometry in EPSG:32632 (UTM zone 32N) |

**`ds_dev_silver.silver_grid_substation`** — MV/LV substations (`voltage_class='mv_lv'`) with the following expected columns:

| Column | Description |
|--------|-------------|
| `dso_id` | UUID of the DSO — stamped from `raw.dso_registry` in the upstream ingestion pipeline |
| `asset_id` | IdentifiedObject.mRID (unique) |
| `name` | Substation name |
| `label_id`, `label` | Display identifiers |
| `line_name`, `feeder_id` | Associated circuit |
| `parent_substation_name` | Upstream HV/MV substation |
| `operational_unit` | Distribution operational unit |
| `municipality` | Administrative municipality |
| `geom` | Geometry in EPSG:32632 |

## dbt models

All four gold models carry a `dso_id` column (UUID) inherited from the silver source. This enables DSO-scoped access control via row-filter handlers and supports future multi-DSO deployments.

### `grid_wind_risks`

Wind risk per overhead MT line segment (all conductor types except `underground_cable`). Spatially joins segments with `om_wind_gusts` (Open-Meteo): nearest weather station within 5 km per segment per date. `DISTINCT ON (date, line_name, municipality, conductor_type, length_m)` keeps the closest station. Covers today + 2 days ahead.

`risk_level` = `ALERT | WARNING | NORMAL` (mapped from `gust_excess_tier`). `feature_geojson` is a ready-to-render GeoJSON Feature with stroke styling and risk metadata in `properties`.

Materialized as **incremental** (`schema: gold`) — historical rows are preserved across runs. Each daily run upserts today + 2 days; unique key on `(date, dso_id, line_name, municipality, conductor_type, length_m)`.

### `grid_heat_risks`

Heat risk per underground cable segment (`conductor_type = 'underground_cable'`). Same spatial join pattern against `om_heat_risk`. Heat risk applies to buried conductors sensitive to soil temperature; overhead lines are excluded.

`risk_level` is normalised from `heat_risk_tier` (`RED → ALERT`, `ORANGE → WARNING`, `GREEN → NORMAL`). Covers today + 2 days ahead.

Materialized as **incremental** (`schema: gold`) — same accumulation strategy as `grid_wind_risks`.

### `grid_substations`

Static GeoJSON map layer for MV/LV substations. No weather join. Projects `silver_grid_substation` to gold with WGS84 `longitude`/`latitude` columns and a `feature_geojson` point feature for map rendering.

Materialized as table (`schema: gold`). Updated on the monthly topology cadence.

### `grid_network_topology`

Distinct topology entities (lines, substations, operational units, municipalities, conductor types) sourced directly from `silver_grid_ac_line_segment`. Powers the `/filters` UI endpoint — ensures all filter options are present even for segments with no nearby weather station. Updated on the monthly topology cadence.

Materialized as table (`schema: gold`).

### Macros

- `grid_risk_color(tier)` — maps `ALERT | WARNING | NORMAL` to a hex colour for map rendering.
- `grid_geojson_feature(geom_col, ...)` — builds a GeoJSON Feature string with risk-aware stroke styling from a geometry column.

## Historical backfill

To populate risk history, sync the upstream source tables first (`om_wind_gusts`, `om_heat_risk`, `silver_grid_ac_line_segment`), then run with a `start_date` override:

```bash
dbt run --select grid_wind_risks grid_heat_risks --vars '{"start_date": "2024-01-01"}'
```

The `date_range` CTE will expand from `start_date` through today + 2 days. The incremental unique key prevents duplicates on re-runs.

## Flow (`flows/pipeline.py`)

`grid-resilience-flow` runs two tasks in sequence: **Transform Gold Layer** (`dbt run --select gold`) followed by **Test grid models**. Schedule is configured in `flows/config.yaml`: cron `0 8 * * *` (daily at 08:00 UTC), after the Open-Meteo wind and heat pipelines complete.
