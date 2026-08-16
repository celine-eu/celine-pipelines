# Development

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Python | ≥ 3.12 | Pipeline runtime |
| [`uv`](https://docs.astral.sh/uv/) | latest | Dependency management |
| [Task](https://taskfile.dev) | latest | Task runner |
| Docker + Docker Compose | latest | Database and container execution |

## Setup

```bash
cp .env.example .env
task setup
```

`task setup` runs `uv sync`, installs every `apps/*/requirements.txt` into the same
environment, and installs `celine-utils` editable from `../celine-utils`. That last step
assumes the sibling checkout exists; adjust `CELINE_UTILS_PATH` in `taskfile.yaml` if
yours is elsewhere.

Then start the database:

```bash
docker compose up datasets-db -d
```

## Running a pipeline

Locally, in your terminal — the usual development loop:

```bash
cd apps/om
source <(uv run celine-utils pipeline run envs)
uv run celine-utils pipeline run prefect -f pipeline_wind
```

See [Local Runtime](local-runtime.md) for the full command set, how the context is
discovered, and what to do about upstream tables you do not have.

## Running in Docker

Each pipeline has a service in `docker-compose.yaml`, built from the shared base image.

```bash
# Build the base image once, then a pipeline
docker compose build pipeline-base
docker compose build pipeline-om

# One-shot run
docker compose run --rm pipeline-om python3 ./flows/pipeline.py

# A non-default flow in the same image
docker compose run --rm pipeline-om python3 ./flows/pipeline_wind.py

# As a scheduled service (PREFECT_MODE=dev makes the flow serve() on its cron)
docker compose up pipeline-om -d
```

Optional supporting services:

```bash
docker compose up marquez-api marquez-web -d     # lineage UI on :5002
docker compose up prefect-server prefect-worker -d
task create-worker-pool                          # one-off: creates the "celine" work pool
```

`apps/` and `../celine-utils` are bind-mounted into the containers, so an edit is visible
without a rebuild. A change to `requirements.txt` still needs one.

### Adding a pipeline's compose service

Every app under `apps/` is expected to have a service in `docker-compose.yaml`, following
the `x-pipeline-service` anchor. Three do not yet: `pv_detection`,
`rec_flexibility_commitments` and `rec_registry`. Adding one is a copy of the nearest
existing block with the app's own `APP_NAME` and command.

## Adding a new pipeline

Follow the [pipeline
tutorial](https://celine-eu.github.io/projects/celine-utils/docs/pipeline-tutorial), which
covers scaffolding with `celine-utils pipeline init`, defining flows, wiring Meltano and
dbt, and adding governance metadata.

A new pipeline in **this** repository additionally needs:

- `governance.yaml` listing only the datasets it produces — duplicating a dataset another
  app already declares is not allowed
- `version.txt`, starting at `0.1.0`
- a service in `docker-compose.yaml`
- a `README.md` — it is the authoritative per-pipeline reference and is published to the
  documentation site
- an entry in [Pipelines Reference](pipelines-reference.md)
- tests: see [Testing](testing.md)

## Versioning and releases

Each pipeline is versioned independently in `apps/<name>/version.txt`; the shared base
image is versioned in `version-base.txt`. CI watches those files: pushing a change to one
builds and publishes that image to `ghcr.io/celine-eu/pipeline-<name>`.

```bash
# Bump one app (minor) and commit
task pipeline:release:app -- om --commit

# Bump every app, then one commit for all of them
task pipeline:release:all

# Bump the shared base image
task pipeline:release:base
```

`task pipeline:release:app -- <app>` without `--commit` writes the file and leaves it
staged for you to review. The underlying script is `scripts/bump_version.py`, which also
accepts `patch` and `major`.

`taskfile.yaml` has named shortcuts (`pipeline:release:om`, `pipeline:release:osm`, …) for
some apps only; the generic `pipeline:release:app -- <name>` form works for all of them.

## Documentation

`README.md`, `apps/*/README.md` and `docs/**` are fetched by
[celine-eu.github.io](https://celine-eu.github.io/projects/celine-pipelines/) and published
as this project's documentation. The nav is declared in that repository's `repos.yaml`, so
**a new page under `docs/` also needs a nav entry there** or it will be fetched and never
linked.

A push to `main` touching those paths triggers the site rebuild via
`.github/workflows/update-docs.yaml`.

## Where else to look

| Looking for | Go to |
|---|---|
| what each pipeline does | [Pipelines Reference](pipelines-reference.md) |
| the layer model, contracts, governance | [Pipeline Overview](pipeline-overview.md) |
| running things locally | [Local Runtime](local-runtime.md) |
| how to verify a change | [Testing](testing.md) |
| why a technical choice was made | [`docs/decisions/`](decisions/index.md) |
| a trap that is true of the code and not obvious from it | [the companion's knowledge](https://github.com/celine-eu/celine-pipelines/tree/main/.agents/knowledge) |
| a repeatable procedure | [the companion's playbooks](https://github.com/celine-eu/celine-pipelines/tree/main/.agents/playbooks) |
| something that is broken | `gh issue list` |
