# Local Runtime

How to run a pipeline — or one stage of it — directly in your terminal, without the
container wrapper.

Everything here is provided by
[`celine-utils`](https://celine-eu.github.io/projects/celine-utils/docs/cli), which is both
the library the flows import and the CLI that reproduces the container's execution context
locally.

## Why run outside the container

The container entrypoint runs one thing: the flow, end to end. That is right for
production and wrong for development, where you usually want to re-run *one* dbt model
against data that is already loaded, or step into a Python task with a debugger. The CLI
gives you the same environment the container would have built, at a shell prompt.

## Prerequisites

### 1. The toolchain

| Tool | Why |
|---|---|
| Python ≥ 3.12 | pipeline runtime |
| [`uv`](https://docs.astral.sh/uv/) | dependency management |
| [Task](https://taskfile.dev) | the repository's task runner |
| Docker + Docker Compose | the database, and container runs |

```bash
task setup
```

`task setup` runs `uv sync`, installs every `apps/*/requirements.txt` into the same
virtualenv, and installs `celine-utils` **editable from `../celine-utils`**. That last
step is deliberate: local pipeline work usually moves both repositories together, and an
editable install means a change to the runner is visible without a reinstall.

Check the CLI resolves:

```bash
uv run celine-utils --help
uv run celine-utils pipeline run --help
```

### 2. The database — required

Every stage below writes to Postgres. Nothing works without it.

```bash
docker compose up datasets-db -d
docker compose ps datasets-db          # must be "healthy"
```

`datasets-db` is `postgis/postgis:17-3.5-alpine` — PostGIS, not plain Postgres, because
`grid` and `trentino_rooftops` need it. It publishes **15432**, not 5432, so it does not
collide with a system Postgres.

Confirm you can reach it the way the pipelines will:

```bash
psql "postgres://postgres:securepassword123@host.docker.internal:15432/datasets" -c '\dn'
```

> **`host.docker.internal` must resolve on the host too.** The defaults use it so the same
> values work inside and outside a container. Outside, it only resolves if your
> `/etc/hosts` maps it to the Docker bridge address:
>
> ```bash
> getent hosts host.docker.internal   # expect e.g. 172.17.0.1
> ```
>
> If it does not resolve, either add that mapping or override `POSTGRES_HOST=localhost`
> in `.env`. A `could not translate host name` error is this, not a database problem.

### 3. Marquez — optional, but decide explicitly

`marquez-api` and `marquez-web` receive the OpenLineage events the runner emits. Lineage
is on by default, and the OpenLineage HTTP transport **retries with backoff** when the
endpoint refuses the connection. So a local run with Marquez down does not fail fast — it
emits an event per stage, waits out the retries on each, and finishes minutes later than
it should.

Pick one, at the start of the session:

```bash
# Track lineage
docker compose up marquez-api marquez-web -d      # UI on http://localhost:5002

# Or don't
export OPENLINEAGE_ENABLED=false
```

Put `OPENLINEAGE_ENABLED=false` in `.env` if you never want it locally.

### 4. `.env`

```bash
cp .env.example .env
```

`.env.example` covers the database and the lineage endpoint. Add whatever the pipeline you
are working on needs on top — `OWM_API_KEY`, `CDSAPI_KEY`/`CDSAPI_URL`, and the schema
overrides below. Each pipeline's `README.md` names its own.

`.env` is gitignored, and stays that way.

## The four subcommands

All of them are run **from inside the app directory**:

```bash
cd apps/rec_metering
```

### `pipeline run envs` — print the context

```bash
uv run celine-utils pipeline run envs
```

Prints, as `export` lines, every variable the runner would set. Source it and your shell
becomes the container's environment — after which bare `dbt`, `meltano` and `python`
work with no wrapper at all:

```bash
source <(uv run celine-utils pipeline run envs)

dbt run --select gold          # works: DBT_PROJECT_DIR and DBT_PROFILES_DIR are set
dbt test
meltano run import
```

A typical result:

```bash
export APP_NAME=rec_metering
export PIPELINES_ROOT=/…/celine-pipelines
export DBT_PROJECT_DIR=/…/celine-pipelines/apps/rec_metering/dbt
export DBT_PROFILES_DIR=/…/celine-pipelines/apps/rec_metering/dbt
export POSTGRES_HOST=host.docker.internal
export POSTGRES_PORT=15432
export POSTGRES_DB=datasets
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=securepassword123
export MELTANO_DATABASE_URI=postgresql://…/meltano
export OPENLINEAGE_URL=http://host.docker.internal:5003
export OPENLINEAGE_ENABLED=True
export MQTT_EVENTS_ENABLED=True
export DO_NOT_TRACK=1
```

This is also the fastest way to debug "why is it reading the wrong schema" — the answer is
in this output.

### `pipeline run meltano` — ingestion

```bash
uv run celine-utils pipeline run meltano                  # default: "run import"
uv run celine-utils pipeline run meltano "run tap-x target-y"
```

Populates the `raw` layer. Only meaningful for apps that have a `meltano/` directory.

### `pipeline run dbt` — transformation

```bash
uv run celine-utils pipeline run dbt staging
uv run celine-utils pipeline run dbt silver
uv run celine-utils pipeline run dbt gold
uv run celine-utils pipeline run dbt test
uv run celine-utils pipeline run dbt "-s gold,tag:wind"
```

The argument is passed to dbt as a selector, so every dbt selection expression works —
including the tag intersections that multi-flow apps depend on.

### `pipeline run prefect` — the whole flow

```bash
uv run celine-utils pipeline run prefect                       # flows/pipeline.py
uv run celine-utils pipeline run prefect -f pipeline_wind      # flows/pipeline_wind.py
uv run celine-utils pipeline run prefect -f pipeline -x om_flow
```

The flow function is auto-detected by scanning the module for a `@flow` decorator; `-x`
overrides it. Prefect runs against a throwaway `PREFECT_HOME` in a temp directory that is
removed afterwards, so a local run leaves no Prefect state behind and needs no Prefect
server.

`apps/om/taskfile.yaml` shows the idiomatic wrapping:

```yaml
run:wind:
  cmds:
    - source <(celine-utils pipeline run envs) && celine-utils pipeline run prefect -f pipeline_wind
```

## How the context is discovered

Worth knowing, because every "it ran against the wrong thing" bug is one of these:

1. **App root** — walk up from the current directory until a folder containing
   `meltano/`, `dbt/` or `flows/` is found. Running from the repository root does not
   work; running from `apps/om/dbt` does.
2. **`APP_NAME`** — the `APP_NAME` environment variable if set, otherwise the app
   folder's name.
3. **`PIPELINES_ROOT`** — the `PIPELINES_ROOT` variable if set; otherwise, when the app
   sits under `apps/`, the repository root.
4. **dbt and Meltano paths** — `DBT_PROJECT_DIR`, `DBT_PROFILES_DIR` and
   `MELTANO_PROJECT_ROOT` are derived from the app root when those directories exist.
5. **`.env` files** — `<repo>/.env`, `<repo>/.env.local`, then `<app>/.env`,
   `<app>/.env.local`. **Later files override earlier ones**, so a per-app `.env` wins
   over the repository one.

`APP_NAME`, `PIPELINES_ROOT`, `MELTANO_PROJECT_ROOT`, `DBT_PROJECT_DIR` and
`DBT_PROFILES_DIR` are *cleared* before discovery. Exporting them by hand will not pin
them — discovery is always driven by the working directory.

## Environment reference

| Variable | Default | Purpose |
|---|---|---|
| `POSTGRES_HOST` | `host.docker.internal` | Database host |
| `POSTGRES_PORT` | `15432` | Published port of `datasets-db` |
| `POSTGRES_DB` | `datasets` | Database name |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | `postgres` / `securepassword123` | Local credentials |
| `CELINE_SILVER_SCHEMA` | `ds_dev_silver` | Where upstream **silver** sources are read from |
| `CELINE_GOLD_SCHEMA` | `ds_dev_gold` | Where upstream **gold** sources are read from |
| `OPENLINEAGE_ENABLED` | `true` | Set `false` to skip lineage entirely |
| `OPENLINEAGE_URL` | `http://host.docker.internal:5003` | Marquez API |
| `MQTT_EVENTS_ENABLED` | `true` | Pipeline status events over MQTT |
| `RAISE_ON_FAILURE` | `true` | Whether a failed task aborts the flow |
| `APP_NAME`, `PIPELINES_ROOT` | discovered | Override discovery (see caveat above) |
| `PREFECT_MODE` | — | `dev` makes the flow `serve()` on its cron instead of running once |

Pipeline-specific credentials (`OWM_API_KEY`, `CDSAPI_KEY`, …) are listed in each app's
`README.md`.

## Providing upstream data locally

Pipelines read each other's gold tables, and a few read tables produced outside this
repository entirely. Either way the rule is the same: **work out who produces the table
before you conclude anything about the pipeline that failed.**

Most upstreams are internal — running the producing app fixes it:

| Run this first | To satisfy |
|---|---|
| `overture` | `pv_detection`, `pv_estimation`, `trentino_rooftops` |
| `om`, `mt` | `grid` |
| `mt` (or any provider) | `weather` |
| `rec_metering` | `rec_it`, `rec_flexibility` |
| `rec_registry` | `rec_it` |
| `rec_flexibility_commitments` | `rec_flexibility` |
| `rec_it` | `pv_estimation` |

The last two rows of that first group are the ones people miss: `raw.rec_registry_mirror`
and `raw.flexibility_commitments_mirror` mirror private *services*, but the apps that do
the mirroring are in this repository.

Five tables genuinely come from outside it — `meters_data_normalized`,
`silver_grid_ac_line_segment`, `silver_grid_substation`, `meters_energy_forecast` and
`total_meters_forecast`. For those:

1. **Run the producer** against the same local database, if you have access to it.
2. **Point at a schema that already has them**, read-only, via `CELINE_SILVER_SCHEMA` /
   `CELINE_GOLD_SCHEMA`.
3. **Create the table by hand** from the DDL in the consuming app's `README.md` and load
   synthetic rows. `sources.yml` is the authoritative column contract.

A dbt error naming a source table is telling you which upstream is absent or stale — it is
not telling you the pipeline is broken. See
[Reading a source failure](pipeline-overview.md#reading-a-source-failure).

## Populating the database from scratch

There is no single command that builds everything. Pipelines depend on each other's gold
tables, and the order comes from what each app declares: `depends_on:` in its
`governance.yaml` names the datasets it reads, `sources:` names what it produces, and
`celine-utils` resolves one against the other.

**Ask for the order rather than reading it here** — it is generated from the files, so it
cannot go stale the way a table in a document does:

```bash
uv run celine-utils governance graph 'apps/*'            # tiers, and anything inconsistent
uv run celine-utils governance graph 'apps/*' -f order   # the same, flattened to a sequence
```

Everything in one tier may run in any order, or in parallel; a tier needs the one before
it. `--strict` exits non-zero if any pipeline declares an input nothing here produces,
which is the form to run in CI. Requires `celine-utils` 2.4 or later; on an older
install, the tiers below are the same graph as of 2026-08-18.

<details>
<summary>The tiers, as the command prints them today</summary>

```text
tier 0   copernicus  dwd  mt  om  osm  overture  owm
         rec_flexibility_commitments  rec_metering  rec_registry
tier 1   grid  pv_detection  rec_flexibility  rec_it  trentino_rooftops  weather
tier 2   pv_estimation
```

Seven declare `active: false`, meaning they are on no schedule anywhere and the tiers
above include them only for completeness:

| | |
|---|---|
| retired | `copernicus`, `dwd`, `owm` — each superseded by another pipeline |
| local only | `overture`, `trentino_rooftops`, `pv_detection`, `pv_estimation` — run on demand against a local database |

The four local-only ones form a connected subtree, so building any of them means building
the ones above it. Skip all seven unless you specifically need what they produce.

</details>

**Five tables come from outside this repository**, so the pipelines that read them cannot
be built from here alone: `rec_metering` needs `meters_data_normalized`, `grid` needs
`silver_grid_ac_line_segment` and `silver_grid_substation`, and `rec_flexibility` needs
`meters_energy_forecast` and `total_meters_forecast`. Each is declared `external: true`
in the consuming app's `depends_on:`, and the graph lists them under *satisfied outside
this scan*. See the companion's knowledge for how to obtain them.

### The shape of one app's build

```bash
cd apps/<name>
source <(uv run celine-utils pipeline run envs)

uv run celine-utils pipeline run meltano      # if the app has meltano/
uv run dbt seed                               # if the app has seeds
uv run dbt build                              # models + their tests, in dbt's own DAG order
```

`dbt build` is the right verb here rather than `dbt run`: it interleaves each model's tests
with the model, so a tier that populated badly fails at the tier that caused it rather than
three tiers later.

Running the flow instead — `uv run celine-utils pipeline run prefect` — does the same work
plus whatever Python tasks the app has, and is what you want for `rec_flexibility`,
`pv_estimation`, `pv_detection` and the two API mirrors.

> **Schedules do not encode this order reliably.** Deployed pipelines are sequenced by
> cron offsets — `mt` at `:05`, `weather` at `:15` — which is a convention, not a
> guarantee. At least one pair is scheduled at the same minute despite a real dependency.
> Never infer the build order from the crons; use the tiers above.

## See also

- [Testing](testing.md) — the layers, and the cross-pipeline cascade
- [Development](development.md) — Docker, releases, adding a pipeline
- [celine-utils CLI](https://celine-eu.github.io/projects/celine-utils/docs/cli)
- [Pipeline tutorial](https://celine-eu.github.io/projects/celine-utils/docs/pipeline-tutorial)
