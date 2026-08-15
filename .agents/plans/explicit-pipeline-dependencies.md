---
slug: explicit-pipeline-dependencies
created: 2026-08-15
status: proposed
requires-new-spec: true
---

# Make inter-pipeline dependencies explicit

**Not scheduled. Analysis only.** The mechanism would live in `celine-utils`, which makes
this a cross-repository decision that nobody has written a requirement for yet. Per the
rulebook this plan stays `proposed` until that conversation happens.

## The problem

`mt` produces the `weather__*` contract tables. `weather` consumes them. Nothing in either
repository says so.

That edge exists 13 times across `apps/`, and it is currently expressed in **three
different places, none of which is authoritative**:

| Where | Example | Visible to |
|---|---|---|
| `dbt/models/**/sources.yml` | `weather` declares `weather__forecast_hourly` | dbt, within one app |
| `apps/<name>/flows/config.yaml` | `pv_estimation` names `pv_building_suitability` as a table | that app's Python only |
| The cron offset | `mt` at `:05`, `weather` at `:15` | nothing |

A dbt `source()` is the closest thing to a declaration, but dbt's DAG stops at the project
boundary: each app is its own dbt project, so `ref()` never crosses apps and `dbt build`
can only order models *within* one pipeline. Across pipelines there is no graph at all.

## What this already costs

**A confirmed ordering bug.** `overture` and `trentino_rooftops` are both scheduled at
`0 2 * * *` — the same minute — while `trentino_rooftops.pv_building_suitability` reads
`overture`'s `pv_overture_buildings`. Whether the consumer sees today's buildings or
yesterday's depends on which container starts first. Nothing detects this, because nothing
knows the edge exists.

**Dependencies that are invisible to the obvious tool.** `pv_estimation` depends on
`trentino_rooftops` and `pv_detection` through its flow config, not its `sources.yml`.
Anyone reconstructing the graph from `sources.yml` — the natural move — gets it wrong.

**Ordering knowledge that only exists in prose.** The tier table in
`docs/local-runtime.md` was reconstructed by hand from 20 `sources.yml` files. It is
correct today and has no mechanism keeping it correct.

**Silent staleness rather than failure.** A consumer whose producer has not run reads the
previous run's output and succeeds. Section
[Reading a source failure](../../docs/pipeline-overview.md#reading-a-source-failure)
exists because this is the most common false diagnosis in the repository.

## The edges, as they stand

Reconstructed 2026-08-15 from every `sources.yml` plus the flow configs.

```text
tier 0   om  mt  owm  copernicus  osm  overture  rec_registry
         rec_flexibility_commitments   rec_metering*
tier 1   weather←mt          trentino_rooftops←overture   pv_detection←overture
         rec_it←rec_metering,rec_registry
         rec_flexibility←rec_metering,rec_flexibility_commitments*
         grid←om,mt*
tier 2   pv_estimation←overture,rec_it,trentino_rooftops,(pv_detection)

* also depends on a producer outside this repository
```

## Options

### A. Declare it in `governance.yaml`

Every app already has one, `celine-utils` already parses it, and it already describes
datasets. Adding the *inputs* a pipeline consumes — not just the outputs it produces —
makes the graph derivable from files that exist.

Cheapest, and it fits the existing shape. But `governance.yaml` is consumed by
`dataset-api` for cataloguing and dataspace exposure, so widening its meaning to
"orchestration input" needs agreement from that side, and the schema in
the `governance.schema.json` published by `celine-utils` is versioned and shared.

### B. A dedicated declaration, resolved by `celine-utils`

A `depends_on:` block naming upstream *datasets* (not pipelines), with `celine-utils`
resolving dataset → producing pipeline from the governance files it already reads. Keeps
governance metadata about governance; keeps the coupling at the dataset level, which is
what the contract actually is.

More moving parts, and a second file per app.

### C. Derive it, declare nothing

Parse every app's `sources.yml` and match against every app's `governance.yaml` outputs.
No new format at all.

Attractive until the `pv_estimation` case: a dependency expressed in Python config is not
in `sources.yml`, so derivation is silently incomplete — the worst failure mode, since it
produces a graph that looks authoritative.

**Leaning A or B, with the C derivation as a checker rather than the source of truth** — a
CI job asserting that every `source()` in every app resolves to a declared dependency would
catch the drift regardless of which of A or B is chosen.

## What it would buy

- **Ordering that is checkable.** A cron-vs-graph consistency check would have caught the
  `overture`/`trentino_rooftops` collision the day it was introduced.
- **A generated tier table**, replacing the hand-maintained one in `docs/local-runtime.md`
  — the rulebook's "a number a command can produce is never written down by hand", applied
  to an ordering.
- **Cascade testing that follows the graph.** `docs/testing.md` currently lists four chains
  by hand; they are a projection of this same graph.
- **Real orchestration later.** Prefect can express upstream/downstream, but only if
  something knows the edges. Today the sequencing is cron offsets and hope.

## Explicitly out of scope

Actually sequencing the deployed pipelines. Making the graph explicit is a prerequisite
for that, not the same work, and scheduling policy is a deployment concern.

## Open questions for whoever picks this up

1. Does `governance.yaml` widen to cover inputs, or does a new declaration appear?
2. Do dependencies name **datasets** or **pipelines**? Datasets match the contract and
   survive a producer being split; pipelines are simpler and are what a scheduler wants.
3. How are external producers declared — the five tables from outside this repository need
   a way to be "declared upstream, satisfied elsewhere" without naming a private repo in
   an open-source file.
4. Who enforces it: a `celine-utils` CLI check, a CI job here, or both?

## Immediate, independent of any of this

The `overture` / `trentino_rooftops` schedule collision is a real defect and does not need
this plan to be fixed — moving one cron is a one-line change. It should be filed as an
issue rather than waiting on the mechanism.
