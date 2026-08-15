# Pipeline Overview

Every CELINE pipeline is a self-contained application under `apps/<name>/`. It bundles
everything needed to ingest, transform, govern and publish one family of datasets: the
extractor configuration, the SQL that refines it, the orchestration that runs it, and the
governance metadata that describes what came out.

## Data layers

| Layer | Purpose | Schema (default) |
|---|---|---|
| **raw** | Verbatim data as received from the source — no transformations | `raw` |
| **staging** | Technical normalization: type casting, deduplication, field renaming | `ds_dev_staging` |
| **silver** | Enriched, curated, domain-ready datasets | `ds_dev_silver` |
| **gold** | Stable, publication-ready interfaces with full governance metadata | `ds_dev_gold` |

`ds_dev` is the dbt profile's base schema; dbt appends the layer suffix. Under the `prod`
target the base becomes `ds_prod`.

The layer boundary that matters is **gold**. Gold is the only layer another pipeline or
service may read: it is the published contract, and changing a gold column is a breaking
change for everything downstream. Silver is internal to the pipeline that produces it —
with one deliberate exception, described under [Cross-pipeline
contracts](#cross-pipeline-contracts) below.

## Tooling per layer

| Concern | Tool | Where it lives |
|---|---|---|
| Source ingestion → raw | Meltano / Singer taps | `apps/<name>/meltano/` |
| raw → staging → silver → gold | dbt (postgres adapter) | `apps/<name>/dbt/` |
| Orchestration, scheduling, retries | Prefect 3 | `apps/<name>/flows/` |
| Lineage and data-quality events | OpenLineage → Marquez | emitted by `celine-utils` |
| Dataset governance and exposure | `governance.yaml` | `apps/<name>/governance.yaml` |

Not every pipeline uses every tool. A pipeline whose input is another pipeline's gold
table has no `meltano/` directory at all (`rec_metering`, `rec_flexibility`, `grid`,
`weather`); a pure API mirror has no `dbt/` directory (`rec_registry`,
`rec_flexibility_commitments`).

## Anatomy of a pipeline app

```text
apps/<name>/
  dbt/
    dbt_project.yml        # project name, layer materializations, path config
    profiles.yml           # postgres connection, built from POSTGRES_* env vars
    models/
      staging/             # views: type casting and renaming
      silver/              # curated tables
      gold/                # published tables and views
      */sources.yml        # the input contract — see below
      */schema.yml         # model descriptions and data tests
    tests/                 # singular (SQL) tests
    seed/ | data/          # dbt seeds
    macros/
  flows/
    pipeline.py            # the Prefect flow; more flows = more files
  meltano/
    meltano.yml            # extractors and loaders producing the raw layer
  governance.yaml          # dataset ownership, licence, exposure, lineage
  requirements.txt         # optional extra Python deps installed into the container
  version.txt              # pipeline version, mapped to the image tag built by CI
  README.md                # the authoritative per-pipeline reference
  taskfile.yaml            # optional local shortcuts
```

`version-base.txt` and `Dockerfile.base` at the repository root build the shared base
image: Python managed by `uv`, with `celine-utils` and the dbt/Meltano/Prefect stack
already installed. Each pipeline image layers only its own `requirements.txt` on top.

## Pipelines are incremental

Most gold models are `materialized='incremental'` with `incremental_strategy='merge'` on a
surrogate `_id`. Cold storage is handled outside dbt by `pg_freezer`, which drops table
rows past a timestamp threshold after safely writing them to Parquet on S3/MinIO. A model
must therefore never assume its own table holds the full history.

## Multi-flow pipelines

One app can host several Prefect flows sharing the same image but running on independent
schedules. Each flow gets its own `flows/pipeline_<name>.py` and its own config file, and
dbt models are separated by **tag** rather than by project:

```text
apps/om/
  flows/
    pipeline.py            # om-flow (weather forecast)
    pipeline_wind.py       # om-wind-flow
    pipeline_heat.py       # om-heat-flow
    pipeline_obs.py        # om-obs-flow
  dbt/models/
    staging/
      stg_om_weather.sql           # weather (untagged)
      wind/stg_om_wind.sql         # tag: wind
    silver/
      om_weather_hourly.sql
      wind/om_wind_hourly.sql      # tag: wind
    gold/
      om_weather_features.sql
      wind/om_wind_gusts.sql       # tag: wind
```

Each flow selects only its own models:

```python
dbt_run("-s staging,tag:wind", cfg)   # wind staging only
dbt_run("-s silver,tag:wind", cfg)
dbt_run("-s gold,tag:wind", cfg)
dbt_run("test -s tag:wind", cfg)
```

Tags are declared in the per-directory `schema.yml`:

```yaml
models:
  - name: om_wind_hourly
    config:
      tags: ["wind"]
```

> **dbt selector semantics.** Inside `--select`, a **comma is intersection** (AND); a
> space, or repeating `-s`, is **union** (OR). This is the opposite of most people's
> first guess, and getting it wrong silently runs the whole project instead of one
> flow's slice.

## Cross-pipeline contracts

Pipelines in this repository read each other, and read tables produced by private
ingestion pipelines that are deliberately **not** part of this open-source repository. Two
mechanisms keep that decoupled.

### Configurable source schemas

A pipeline never hard-codes the schema it reads from. `sources.yml` resolves it from the
environment, so the same code runs against any deployment's naming:

```yaml
sources:
  - name: metering_silver
    schema: "{{ env_var('CELINE_SILVER_SCHEMA', 'ds_dev_silver') }}"
    tables:
      - name: meters_data_normalized
```

| Variable | Default | Meaning |
|---|---|---|
| `CELINE_SILVER_SCHEMA` | `ds_dev_silver` | Where upstream silver tables are read from |
| `CELINE_GOLD_SCHEMA` | `ds_dev_gold` | Where upstream gold tables are read from |

The `raw` schema is fixed and not configurable.

### `sources.yml` is the contract

For a pipeline whose upstream is not in this repository, `sources.yml` **is** the
published interface: it names the tables and the columns the upstream must materialise.
The corresponding `README.md` states the expected DDL so the pipeline can be run against
synthetic or locally-loaded data with no access to the producer at all.

This is what makes the repository open source despite depending on data that is not. A
deployment can substitute its own producer — a different DSO's CIM export, a different
metering infrastructure — as long as it satisfies the contract. Nothing about any
particular deployment is encoded here.

### Reading a source failure

Because sources are produced elsewhere, **the pipeline that reports the error is rarely
the one at fault**. Three failures look like a broken model and are not:

| Symptom | What it usually means |
|---|---|
| `source ... not found` | The producer has never run against this database |
| `column "x" does not exist` | This database's copy of the table predates a producer change |
| Model succeeds, output is empty | The source is present but has no rows — and every test over the result then passes vacuously |

So the first move on any of these is to establish **who produces the table**, not to edit
the model that raised it. Often the producer is another pipeline in this repository — run
it first. `grid` needs `om` and `mt`; `rec_it` and `rec_flexibility` need `rec_metering`,
`rec_registry` and `rec_flexibility_commitments`; `weather` needs a provider pipeline; the
PV apps need `overture`.

A consumer should not paper over a missing upstream column with a defensive `coalesce`. A
pipeline that tolerates a broken contract loses the ability to report that it is broken.

The triage sequence and the full producer map are in
[`.agents/knowledge/upstream-tables-have-external-producers.md`](https://github.com/celine-eu/celine-pipelines/blob/main/.agents/knowledge/upstream-tables-have-external-producers.md);
[`.agents/knowledge/grid-strike-tree-columns-are-upstream.md`](https://github.com/celine-eu/celine-pipelines/blob/main/.agents/knowledge/grid-strike-tree-columns-are-upstream.md)
is one case written up in full.

### Contract tables (provider-neutral aliases)

Where several producers feed one consumer, the producers write to a **shared alias**
rather than the consumer learning each producer's table name. The `weather` pipeline uses
this: provider pipelines materialise their gold model under a generic alias,

```sql
{{ config(alias = 'weather__forecast_hourly') }}
```

and `weather` reads only `weather__*`. Adding a provider means the new pipeline writes to
the same alias — the consumer needs no change.

## governance.yaml

Every app carries a `governance.yaml` declaring ownership, licence, access level,
classification, retention, DCAT catalogue metadata and dataspace exposure for each dataset
it produces. `celine-utils` loads it at run time and attaches it to the OpenLineage events;
`dataset-api` reads the same file to build the DCAT catalogue and register dataspace
assets.

```yaml
defaults:
  license: null
  ownership:
    - name: rec
      type: DATA_OWNER
  access_level: internal
  classification: green
  retention_days: 365
  source_system: rec-metering

sources:
  datasets.ds_dev_gold.meters_data_15m:
    tags: [meters, gold, aggregated]
    row_filters:
      - handler: rec_registry
        args:
          column: device_id
    expose: true
```

Two rules are load-bearing:

- **Each pipeline lists only the datasets it produces.** A dataset declared in two
  `governance.yaml` files has two answers to "who owns this", and nothing reconciles them.
- **A dataset with `row_filters` must keep the filtered column.** `dataset-api` applies
  whatever filters a dataset's own entry declares and never compares two datasets; a view
  that dropped the column would be served **unfiltered** rather than failing.

The full schema is published at
[`celine-utils/schema/governance.schema.json`](https://celine-eu.github.io/schema/) and
the field reference in the
[governance documentation](https://celine-eu.github.io/projects/celine-utils/docs/governance).
The `ownership` values map to entries in `dataset-api`'s `owners.yaml`.

## Where to go next

- [Pipelines Reference](pipelines-reference.md) — what each pipeline does and what it produces
- [Local Runtime](local-runtime.md) — running a pipeline on your own machine
- [Testing](testing.md) — the test layers and the cross-pipeline cascade
- [Development](development.md) — setup, Docker, releases
