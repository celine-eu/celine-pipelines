# A missing or wrong source table is usually a stale producer, not a bug here

Verified 2026-08-15 against every `sources.yml` in `apps/`.

Several pipelines here read tables **this repository does not produce**. When one of those
is absent, or is missing a column, or holds nothing, the error surfaces in the *consuming*
pipeline — which is the last place the fault actually lies.

```text
Database Error   column "strike_tree_tier" does not exist
Compilation Error  ... source 'metering_silver'.'meters_data_normalized' ... not found
```

Neither is a defect in the model that raised it. **Check the producer before concluding
anything.** The default assumption — "the model I am looking at is wrong" — costs an
afternoon and ends at a table nobody in this repository writes.
[`grid-strike-tree-columns-are-upstream.md`](grid-strike-tree-columns-are-upstream.md) is
one instance of this written up in full; it is the shape all of them take.

## Who produces what

**External — a private deployment repository, at `{{UPSTREAM_PIPELINES_ROOT}}`:**

| Table | Schema | Consumed by |
|---|---|---|
| `meters_data_normalized` | silver | `rec_metering` |
| `silver_grid_ac_line_segment` | silver | `grid` |
| `silver_grid_substation` | silver | `grid` |
| `meters_energy_forecast` | gold | `rec_flexibility` |
| `total_meters_forecast` | gold | `rec_flexibility` |

The first three come from the deployment's own ingestion app; the two forecasts come from
its `meter_forecasting` app. `{{UPSTREAM_PIPELINES_ROOT}}` is declared in
[`../references.md`](../references.md) and resolves in `references.local.md`. **Never
write the path, the repository name, or anything under it into a committed file here** —
this repository is open source.

**Internal — another app in this repository. No private repo involved:**

| Table | Produced by | Consumed by |
|---|---|---|
| `meters_data_15m` | `rec_metering` | `rec_it`, `rec_flexibility` |
| `om_wind_gusts`, `om_heat_risk`, `om_obs_15min` | `om` | `grid` |
| `heat_daily_obs` | `mt` (model `mt_heat_daily_obs`, aliased) | `grid` |
| `weather__forecast_hourly`, `weather__forecast_daily`, `weather__alerts`, `weather__current` | `mt` (aliased) | `weather` |
| `gse_cabine_primarie` (gold) | `rec_it` | `pv_estimation` |
| `pv_overture_buildings` | `overture` | `pv_detection`, `pv_estimation`, `trentino_rooftops` |
| `raw.rec_registry_mirror` | **`rec_registry`, in this repository** | `rec_it` |
| `raw.flexibility_commitments_mirror` | **`rec_flexibility_commitments`, in this repository** | `rec_flexibility` |
| `_rec_device_baselines_raw`, `_rec_device_streaks_raw` | `rec_flexibility`'s own Python tasks | `rec_flexibility` dbt models |

The two mirrors are the trap in that table. Both are produced by apps sitting next to
their consumer, and both are easy to mistake for private upstreams because they mirror
private *services* — a missing `rec_registry_mirror` means `apps/rec_registry` has not
run, not that something external is stale.

## Why an alias hides the producer

`heat_daily_obs` and the four `weather__*` tables are `mt` models published under a
different name via dbt `alias`. `grep -rn heat_daily_obs apps/mt` finds the alias in the
config block, not the filename — so a search by table name looks like it found nothing and
the table looks external. It is not. Grep for the alias string, not just filenames.

## Triage, in order

1. **Does the table exist?**
   ```bash
   PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
     -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\d ds_dev_silver.<table>'
   ```
   Absent → the producer has never run here. Present but missing a column → the producer
   moved and this checkout's table predates it.
2. **Is it internal?** Check the table above. If so, run that app and stop.
3. **Is it stale rather than absent?** Compare `sources.yml` — which is the *contract*,
   naming the columns this repository requires — against the live table. A column in
   `sources.yml` and not in the table is the producer being behind; the reverse is
   harmless.
4. **Is it just empty?** An incremental model over an empty source succeeds and produces
   nothing, and every test over the result passes vacuously. Check `count(*)` before
   reporting a green run.
5. **Only then** look at the model that raised the error.

## What to report

Say which of the five it was. "The `grid` gold models fail because this checkout's
`silver_grid_ac_line_segment` predates the upstream change that added `strike_tree_tier`"
is actionable. "The grid pipeline is broken" sends someone to rewrite a correct model.

If the producer really is stale, the fix belongs in the producer, not in a defensive
`coalesce` here. A consumer that papers over a missing upstream column stops being able to
tell you the upstream is missing.
