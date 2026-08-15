# Playbook — running a pipeline locally

The commands, the discovery rules and the full environment reference are in
[`docs/local-runtime.md`](../../docs/local-runtime.md). This playbook is the order to do
things in and the failures to expect.

## First time on a machine

```bash
cp .env.example .env
cp .agents/references.local.md.example .agents/references.local.md   # then fill it in
task setup                       # uv sync + apps/*/requirements.txt + editable celine-utils
docker compose up datasets-db -d
docker compose ps datasets-db    # must report "healthy"
uv run celine-utils pipeline run --help
```

`references.local.md` resolves the `{{NAMES}}` cited in these playbooks — currently only
`{{UPSTREAM_PIPELINES_ROOT}}`. Both it and `.env` are gitignored; see
[`../references.md`](../references.md).

Then check the database is reachable *the way the pipelines will reach it*:

```bash
getent hosts host.docker.internal
PGPASSWORD=securepassword123 psql -h host.docker.internal -p 15432 -U postgres -d datasets -c '\dn'
```

If `host.docker.internal` does not resolve on the host, either map it to the Docker bridge
address in `/etc/hosts` or set `POSTGRES_HOST=localhost` in `.env`. A
`could not translate host name` error is this, and nothing else.

## Every session

Decide about lineage **before** the first run, not after wondering why everything is slow:

```bash
docker compose up marquez-api marquez-web -d     # keep it, UI on :5002
# or
export OPENLINEAGE_ENABLED=false                 # skip it
```

See [`knowledge/lineage-retries-stall-local-runs.md`](../knowledge/lineage-retries-stall-local-runs.md)
for why the middle option — Marquez down, lineage on — is the one to avoid.

## Running something

Always from inside the app directory. Context discovery walks *up* from the working
directory, so the repository root is not a valid place to run from.

```bash
cd apps/<name>
source <(uv run celine-utils pipeline run envs)

uv run celine-utils pipeline run meltano          # raw
uv run celine-utils pipeline run dbt staging      # then silver, gold
uv run celine-utils pipeline run prefect          # or the whole flow
```

Once the environment is sourced, bare `dbt` and `meltano` work with no wrapper — which is
what you want while iterating on one model:

```bash
dbt run --select gold
dbt build --select gold          # models + their tests
```

For a multi-flow app, name the flow and select by tag:

```bash
uv run celine-utils pipeline run prefect -f pipeline_wind
uv run celine-utils pipeline run dbt "-s gold,tag:wind"
```

## When it does the wrong thing

**Diagnose with `envs` first.** It prints exactly the context the runner built, and almost
every "it ran against the wrong schema / wrong app / wrong project" answer is visible in
that output:

```bash
uv run celine-utils pipeline run envs
```

| Symptom | Cause |
|---|---|
| `Unable to determine app folder` | Run from inside `apps/<name>/`, not the repository root |
| Wrong `APP_NAME`, or exported values ignored | `APP_NAME`, `PIPELINES_ROOT`, `DBT_*` are cleared before discovery — they are set *from* the working directory, not read from your shell |
| Reads the wrong schema | `CELINE_SILVER_SCHEMA` / `CELINE_GOLD_SCHEMA`; and remember an app-level `.env` overrides the repository one |
| dbt cannot find the project | You are not in the app directory, or the app has no `dbt/` |
| Source relation does not exist, or a column is missing | A producer has not run or is stale — see below. Do not start by editing the model that raised it |
| Everything is slow, nothing fails | Lineage retries; see the knowledge note |

## Populating the database in the first place

This playbook runs *one* app. Building the whole local database, in dependency order, and
recognising when it has drifted is
[`populating-the-database.md`](populating-the-database.md). Go there first on a fresh
machine, or when more than one app is failing.

## Upstream tables you do not have

**Before concluding anything about the pipeline that raised the error, find out who
produces the table.** A missing table, a missing column or an empty result almost always
means a producer has not run or has moved on — not that the model in front of you is
wrong. The producer map and the five-step triage are in
[`knowledge/upstream-tables-have-external-producers.md`](../knowledge/upstream-tables-have-external-producers.md).

The short version:

1. **Is the producer in this repository?** Often yes, and that is the fastest fix. Run
   `rec_metering` before `rec_it`/`rec_flexibility`, `om` and `mt` before `grid`, `mt`
   before `weather`, `overture` before the PV apps, `rec_registry` and
   `rec_flexibility_commitments` before their consumers. The last two are the ones people
   miss — they mirror private *services*, but the mirroring apps live here.
2. **Is it external?** Five tables are: `meters_data_normalized`,
   `silver_grid_ac_line_segment`, `silver_grid_substation`, `meters_energy_forecast` and
   `total_meters_forecast`. Their producers are in the private deployment repository at
   `{{UPSTREAM_PIPELINES_ROOT}}` (declared in [`../references.md`](../references.md),
   resolved in `references.local.md`). Run the producer there against the same local
   database, or fall back to option 3 or 4.
3. **Point at an existing schema** read-only via `CELINE_SILVER_SCHEMA` /
   `CELINE_GOLD_SCHEMA`.
4. **Create the table by hand** from the DDL in the consuming app's `README.md`, and load
   synthetic rows. `sources.yml` is the authoritative contract.

Never write to a schema you did not create. Ask before anything that is not a read.

**Nothing about the private repository — its name, its path, its app names, its data —
goes into a committed file here.** Describe an upstream by its role: "the pipeline that
produces `meters_data_normalized`". The table names themselves are already public; this
repository's own `sources.yml` declares them.

## Do not commit

`.env` and `.env.local` are gitignored and stay that way. Nothing naming a deployment, an
organisation, a customer or a person belongs in a committed file in this repository — it
is open source. If a value is needed to explain something, cite a name in
`{{DOUBLE_BRACES}}` and declare it in `.agents/references.md`.
