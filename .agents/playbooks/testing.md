# Playbook — testing a change

The layers, their commands and what each one proves are documented in
[`docs/testing.md`](../../docs/testing.md); the environment they need is in
[`docs/local-runtime.md`](../../docs/local-runtime.md). This playbook is the procedure —
what to run, in what order, and what usually goes wrong.

Nothing here is runnable without the local runtime. Set it up once per session:

```bash
docker compose up datasets-db -d
cd apps/<name>
export OPENLINEAGE_ENABLED=false          # unless you started marquez-api
source <(uv run celine-utils pipeline run envs)
```

## Before touching anything

Run the suite that covers what you are about to change, and **write the number into
`.agents/work/<slug>/status.md`**:

```bash
uv run dbt test 2>&1 | tail -5            # dbt layers
uv run pytest tests -q                    # only where apps/<name>/tests exists
```

A suite that was already red stays attributable to whoever made it red. Record the counts,
not a "looked fine".

## The order

1. **Baseline**, as above.
2. **Make the change.**
3. **Rebuild and test the app you edited** — `dbt build --select <layer>` runs models and
   their tests interleaved, which is what you want while iterating.
4. **Run the cascade for the chain your change is in.** The four chains, and the exact
   command sequence for each, are in
   [`docs/testing.md` § The cascade](../../docs/testing.md#the-cascade). Do not skip this
   because the app you edited is green: gold tables are read by other apps, and the break
   surfaces one pipeline away from the edit.
5. **Smoke-run the flow** — `uv run celine-utils pipeline run prefect` — if you changed
   anything about how stages are wired.
6. **Report**, per the section below.

## Which layer a new test belongs in

| The thing you want to assert | Layer | Where it goes |
|---|---|---|
| A key is unique, populated, or resolves to another model | generic | `models/**/schema.yml`, under `data_tests:` |
| A domain invariant across the built table | singular | `dbt/tests/<invariant>.sql` |
| Model logic on a data situation that is awkward to arrange | unit | a `unit_tests:` block beside the model |
| A Python task's arithmetic | pytest | `apps/<name>/tests/` |
| The upstream is still being loaded | freshness | `freshness:` on the source in `sources.yml` |

Two rules that are easy to get wrong:

- Use `data_tests:`, not the deprecated `tests:` key, and put generic-test parameters
  under an `arguments:` block. dbt ≥ 1.10 deprecates the flat form and will eventually
  reject it.
- **Only `apps/om` installs `dbt_utils`.** Anywhere else, express a composite-key or range
  assertion as a singular test rather than adding a package dependency to that app's
  image.

Name a singular test after the invariant, not the model, and put the reason on line one.
A test whose failure message does not say what is now untrue costs more than it saves.
`apps/rec_metering/dbt/tests/` and `apps/rec_flexibility/dbt/tests/` are the reference for
style.

## Traps

**An empty table passes every test it has.** Check the row count before believing a pass —
and if a whole cascade stage looks suspiciously clean, the database is probably not
populated. Building it in dependency order is
[`populating-the-database.md`](populating-the-database.md).

```bash
PGPASSWORD=$POSTGRES_PASSWORD psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "select count(*) from ds_dev_gold.<model>;"
```

**A failing source is not a bug in the pipeline you are looking at.** A missing table, a
missing column or an empty result almost always means a producer has not run or has moved
on. Find out who produces the table *before* concluding anything — often it is another app
in this repository, and sometimes it is a private one. Producer map and triage steps:
[`knowledge/upstream-tables-have-external-producers.md`](../knowledge/upstream-tables-have-external-producers.md);
worked example:
[`knowledge/grid-strike-tree-columns-are-upstream.md`](../knowledge/grid-strike-tree-columns-are-upstream.md).

**A slow local run is usually lineage, not the query.** See
[`knowledge/lineage-retries-stall-local-runs.md`](../knowledge/lineage-retries-stall-local-runs.md).

**`rec_flexibility` does not even parse on a fresh checkout.** Its fleet seed is generated
at flow start and is not in the repository. See
[`knowledge/rec-flexibility-needs-its-fleet-seed.md`](../knowledge/rec-flexibility-needs-its-fleet-seed.md)
— it also covers why dbt unit tests need the tables to exist.

**A dbt selector with a comma is an intersection, not a union.** `-s gold,tag:wind` is
"gold AND wind". Getting it backwards runs the whole project and looks like it worked.

## Declaring what a test verifies

This repository does not number requirements, and `.agents/harness.toml` declares no
traceability provider. There is no `@verifies` marker to add — a test is tied to what it
covers by its name and its header comment, and that is the whole contract. If requirement
identifiers are ever introduced here, this section and `harness.toml` change together.

## Reporting

Name the layers that ran, the layers that did not, and why. A layer skipped because it
needs infrastructure you did not start is a fact about the evidence, not an admission.

**A green run is only evidence about what actually ran.** State the row counts the tests
ran against whenever a pipeline's upstream was unavailable — "20/20 passed" and "20/20
passed over 0 rows" are different claims, and only one of them is verification.

**Attribute a failure before reporting it.** "The pipeline is broken" and "this checkout's
copy of an upstream table predates a producer change" call for work in different
repositories. Name which one, and name the producer.
