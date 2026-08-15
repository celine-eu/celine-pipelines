# `strike_tree_*` columns come from upstream, not from this repository

Verified 2026-08-11.

The `apps/grid` gold models — `grid_shapes`, `grid_wind_risks`, `grid_wind_risks_now` and the
`grid_risks*` JSON payloads — read `strike_tree_tier`, `strike_tree_multiplier` and
`strike_density_per_km` off `ds_dev_silver.silver_grid_ac_line_segment`.

**Nothing in celine-pipelines produces those columns.** They are ingested and annotated in the
private deployment repository: tree-strike exposure spans land in a raw table, are staged and
promoted to a silver table, and are joined laterally into `silver_grid_ac_line_segment` taking
the worst intersecting span, with `NULL` meaning no coverage.

## What the error actually means

```
Database Error ... column "strike_tree_tier" does not exist
```

means the **local silver table predates that upstream change** — not a bug in `apps/grid`. The
fix is to refresh the silver interface, never to edit the grid models. People lose an afternoon
here because the failure surfaces in this repository and the cause is not in it.

## The trap that makes refreshing look broken

`on_schema_change` defaults to `ignore`, and `silver_grid_ac_line_segment` is `incremental` with
`merge`. So re-running the upstream dbt model does **not** add the new columns to an existing
table. That route needs `--full-refresh`.

## The fast path

Syncing the already-built silver tables from dev avoids both S3 access and a dbt run. The sync
tool's schema alignment adds missing columns automatically, so `--full-refresh` is *not* needed
on this path. It does require the dev database tunnel, which is usually down.

The three tables to sync together are `silver_grid_ac_line_segment`, `silver_grid_substation`
and `silver_grid_geo_tree_strike`; omitting a date column gives a full truncate-and-reload.
