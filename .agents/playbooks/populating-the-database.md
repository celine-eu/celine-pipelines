# Playbook — populating and realigning the local database

The tiered build order, and what each app needs, is in
[`docs/local-runtime.md` § Populating the database from scratch](../../docs/local-runtime.md#populating-the-database-from-scratch).
This playbook is the procedure around it: how to tell that the database has drifted, what
to do about it, and what not to do.

**Drift is the normal state, not an incident.** A local database accumulates tables built
by different versions of the models at different times, against source data that has since
moved. Most "the pipeline is broken" reports start here.

## Recognising drift

Four symptoms, four different causes. Identify which one before touching anything —
[`../knowledge/upstream-tables-have-external-producers.md`](../knowledge/upstream-tables-have-external-producers.md)
has the full triage.

| Symptom | Cause | Fix |
|---|---|---|
| `source ... not found` | The producer has never run here | Run the producer (find it in the tier table) |
| `column "x" does not exist` | Your table predates a producer change | Rebuild the producer, then the consumer |
| Model succeeds, output empty | Source present but empty, or filtered out by a time window | Check `count(*)` and the model's `where` clause before assuming |
| Tests pass over nothing | Same, one step later | Always report the row count alongside a pass |

The fourth is the dangerous one, because it looks like success.

## Before a build: know where you are

```bash
cd apps/<name>
export OPENLINEAGE_ENABLED=false          # unless marquez-api is up
source <(uv run celine-utils pipeline run envs)

PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "
    select relname, n_live_tup
    from pg_stat_user_tables
    where schemaname = 'ds_dev_gold'
    order by relname;"
```

Row counts before, row counts after. A build that changed nothing and a build that emptied
a table look identical in dbt's output.

## Building

Run the tiers in order. Within a tier, order does not matter.

```bash
cd apps/<name>
source <(uv run celine-utils pipeline run envs)

uv run celine-utils pipeline run meltano    # if apps/<name>/meltano/ exists
uv run dbt seed                             # if the app has seeds
uv run dbt build                            # models + tests interleaved
```

Use `dbt build`, not `dbt run`. It runs each model's tests immediately after the model, so
a tier that populated badly fails where it broke rather than three tiers downstream.

For apps with Python tasks — `rec_flexibility`, `pv_estimation`, `pv_detection`,
`rec_registry`, `rec_flexibility_commitments` — run the flow instead, which does the dbt
work plus the tasks:

```bash
uv run celine-utils pipeline run prefect
```

## `--full-refresh` is not a repair tool

The instinct when a table looks wrong is to rebuild it from scratch. For several models
here that **destroys data**, because the model's own `where` clause is a moving window:

```sql
where f.forecast_at >= now() - interval '1 hour'     -- mt_weather_forecast_hourly
where l.observed_at >= now() - interval '6 hours'    -- mt_weather_current
```

An incremental model with a filter like that accumulates history run by run. Full-refreshing
it rebuilds from the filter alone, so on a database whose source data is older than the
window the result is an **empty table**. Verified: full-refreshing the mt weather contract
on stale local data yields zero rows.

Before `--full-refresh`, read the model's `where`. If it references `now()` or `current_*`,
do not full-refresh it — re-run it normally and let it merge, or fix the rows in place.

The same applies to `materialized='table'` models carrying a time filter: those rebuild
completely on *every* run, so a plain `dbt run` on stale data empties them. That is the
model working as written, not a fault, but it is worth knowing before you run it and lose
your test fixture.

## Adding a column to an incremental model

Set `on_schema_change = 'append_new_columns'` on the model. The column then appears on the
next run — but **existing rows keep NULL until their key is merged again**, and per the
section above you cannot always full-refresh your way out.

For a forecast table this self-heals: each run merges the live window, so NULLs age out
within a forecast horizon. Where a genuine backfill is needed, update in place from the
source rather than rebuilding:

```sql
update ds_dev_gold.weather__forecast_hourly w
set    elevation_m = l.elevation_m
from   ds_dev_silver.mt_forecast_locations l
where  l.location_id::text = w.location_id;
```

## Seeds that are generated, not committed

`rec_flexibility` writes its fleet seed at flow start and it is not in the repository, so
bare `dbt` commands fail at parse time on a fresh checkout. See
[`../knowledge/rec-flexibility-needs-its-fleet-seed.md`](../knowledge/rec-flexibility-needs-its-fleet-seed.md).
An empty seed parses and then fails at run time on a column type — that note has the
generation command.

## Traps

- **Do not infer build order from the cron schedules.** They express dependency by offset
  (`mt` at `:05`, `weather` at `:15`), which is a convention nothing enforces, and at
  least one producer/consumer pair is scheduled at the same minute. Use the tier table.
- **A dependency may not be in `sources.yml`.** `pv_estimation` reads
  `pv_building_suitability` and `pv_detected_installations` through its flow
  `config.yaml`, in Python — invisible to dbt. Grep the app's config as well as its
  sources before deciding what it needs.
- **`dbt unit tests need the tables to exist**, even though they read no rows: dbt infers
  fixture column types from the relation. Build before unit-testing.
- **Lineage retries make a build look hung.** See
  [`../knowledge/lineage-retries-stall-local-runs.md`](../knowledge/lineage-retries-stall-local-runs.md).

## Reporting

State the row counts the build produced, not just that it completed. "20/20 tests passed"
and "20/20 tests passed over 0 rows" are different claims, and only one is verification.

If you rebuilt something and it came out empty, say so explicitly — that is a change to
the shared local database, and the next person's build depends on it.
