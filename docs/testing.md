# Testing

A pipeline can be wrong in four different ways, and each one is caught by a different
layer. Running only the convenient layer and calling the change verified is the failure
this page exists to prevent.

All commands assume the local runtime is set up — see [Local Runtime](local-runtime.md) —
and are run **from inside the app directory** with the environment sourced:

```bash
cd apps/<name>
source <(uv run celine-utils pipeline run envs)
```

## The layers

| Layer | Command | Proves | Needs data |
|---|---|---|---|
| **Python unit tests** | `uv run pytest apps/<name>/tests` | The Python tasks compute what they claim, on synthetic input | no |
| **dbt unit tests** | `dbt test --select "test_type:unit"` | A model's SQL logic is right, on fixture rows | the tables must **exist** |
| **dbt generic tests** | `dbt test --select "test_type:generic"` | Keys are unique and populated, values are in range, references resolve | yes |
| **dbt singular tests** | `dbt test --select "test_type:singular"` | The model's own invariants hold across the built table | yes |
| **source freshness** | `dbt source freshness` | The upstream is actually being loaded | yes |
| **Flow smoke run** | `celine-utils pipeline run prefect` | The stages wire together, and the flow completes | yes |
| **Cascade** | see [below](#the-cascade) | A change to one pipeline's gold layer did not break its consumers | yes |

`dbt build` runs models and their tests interleaved, which is the right default when you
are rebuilding anyway:

```bash
dbt build --select gold
```

### Python unit tests

Pure-Python tasks — baselines, streaks, auto-commit, ROI estimation — are tested with
pytest against synthetic frames, no database. `apps/rec_flexibility/tests/` is the
reference: fixtures in `conftest.py` build a deterministic 3-device × 7-day × 96-slot
meter frame, and `test_python_sql_equivalence.py` asserts the Python task and the dbt
model agree on the same input — the test that catches the two implementations drifting.

```bash
uv run pytest apps/rec_flexibility/tests -q
```

### dbt unit tests

Declared in a `unit_tests:` block, they run a model against fixture rows with no table
behind it. Use them for **logic that would need a specific and awkward data situation** to
exercise: proportional redistribution when the budget is exceeded, a window spanning a day
boundary, a null-heavy edge case. `apps/rec_flexibility/dbt/models/gold/unit_tests.yml` is
the reference.

```yaml
unit_tests:
  - name: w6_normalised_kwh_over_budget
    model: rec_flexibility_windows
    overrides:
      macros:
        is_incremental: false      # required: fixtures have no {{ this }} to read
    given:
      - input: ref('rec_flexibility_windows_community')
        rows:
          - {ts_date: '2024-06-01', window_start: '2024-06-01 09:00:00', community_kwh: 2.0}
    expect:
      rows:
        - {device_id: 'dev-01', estimated_kwh: 0.50}
```

> `overrides.macros.is_incremental: false` is not optional on an incremental model. Without
> it the model compiles its `{% if is_incremental() %}` branch, which reads `{{ this }}` —
> a table the unit test has not built.

> **Unit tests still need the tables to exist**, even though they read no rows from them:
> dbt infers the fixture's column types from the relation. On a project whose models have
> never been built, every unit test errors with `Not able to get columns for unit test …
> because the relation doesn't exist`. That is a missing `dbt run`, not a broken test —
> run the models once, then the unit tests.

### dbt generic tests

Declared per column in `schema.yml`, under `data_tests:` — not the older `tests:` key.

```yaml
models:
  - name: meters_data_15m
    columns:
      - name: _id
        data_tests:
          - unique
          - not_null
      - name: device_id
        data_tests:
          - not_null
```

Built-ins are `unique`, `not_null`, `accepted_values` and `relationships`. `dbt_utils`
adds `unique_combination_of_columns` and `accepted_range`, but **only `apps/om` installs
it** — in every other app, express a composite-key or range assertion as a singular test
rather than adding a package dependency to that app's image.

### dbt singular tests

A `.sql` file under `dbt/tests/` that selects the rows which must not exist. Empty result
means pass. This is where the domain invariants go — the statements that are true of the
data by definition, and whose violation means an upstream or a model is wrong:

```sql
-- Settlement points must never be negative.
select _id, device_id, ts, settlement_points
from {{ ref('rec_settlement_points') }}
where settlement_points < 0
```

Name the file after the invariant, not the model, and put the reason in a comment on line
one. `apps/rec_flexibility/dbt/tests/` holds nine of these and is the reference for style.

### Source freshness

Declared on the source, and the cheapest possible check that an upstream is still being
loaded:

```yaml
sources:
  - name: metering_silver
    tables:
      - name: meters_data_normalized
        loaded_at_field: ts
        freshness:
          warn_after: {count: 30, period: minute}
          error_after: {count: 2, period: hour}
```

```bash
dbt source freshness
```

A stale source explains a downstream emptiness that would otherwise look like a modelling
bug. Run it *first* when a gold table is unexpectedly empty.

### Flow smoke run

```bash
uv run celine-utils pipeline run prefect
```

Proves the stages are wired together — that the flow's dbt selectors match models that
exist, that the Python tasks import, that the sequence completes. It proves nothing about
whether the numbers are right; that is what the layers above are for.

## Establish the baseline first

Before changing anything, run the layer that covers what you are about to touch and record
the result. A suite that was already red stays attributable to whoever made it red;
skipping this step is how a pre-existing failure becomes "the change broke it".

```bash
cd apps/rec_metering
source <(uv run celine-utils pipeline run envs)
dbt test 2>&1 | tail -20        # ← the baseline
```

## The cascade

Pipelines read each other's **gold** tables. A change to a gold model is therefore a
change to every pipeline downstream of it, and testing only the app you edited proves
nothing about them.

Four chains exist. Find the one containing your change and run it left to right. If the
database is empty rather than merely stale, build it first —
[Populating the database from scratch](local-runtime.md#populating-the-database-from-scratch)
gives the full tiered order, of which these chains are a projection.

### Chain 1 — REC

```text
[private metering]  →  rec_metering  →  rec_it
                                     →  rec_flexibility
```

```bash
# 1. the interface
cd apps/rec_metering
source <(uv run celine-utils pipeline run envs)
dbt source freshness
dbt build --select gold                    # models + their tests

# 2. the Italian settlement consumer
cd ../rec_it
source <(uv run celine-utils pipeline run envs)
uv run celine-utils pipeline run meltano   # gse_cabine_primarie, self-contained
dbt build --select silver gold

# 3. the flexibility consumer
cd ../rec_flexibility
source <(uv run celine-utils pipeline run envs)
uv run pytest tests -q

# The private fleet seed is generated at flow start and is not in the repository.
# Without it dbt cannot even parse this app. Set REC_ACTIVE_DEVICES first — an
# empty fleet parses but fails at run time with `operator does not exist:
# text = integer`, because dbt types an empty seed column as integer.
uv run python -c "
import sys; sys.path.insert(0, '.')
from pathlib import Path
from lib.config import get_active_devices, load_config, write_active_devices_seed
print(write_active_devices_seed(get_active_devices(load_config()), Path('dbt/seeds/rec_active_devices.csv')))
"

dbt build                                  # unit + generic + singular, all layers
```

`rec_flexibility` also depends on `meters_energy_forecast` and `total_meters_forecast`,
which no pipeline in this repository produces. Without them the windows models are empty
and their tests pass vacuously — which is a pass about nothing. Say so when reporting.

### Chain 2 — weather facade

```text
mt (and any other provider)  →  weather__* contract tables  →  weather  →  digital-twin, celine-webapp
```

```bash
cd apps/mt
source <(uv run celine-utils pipeline run envs)
uv run celine-utils pipeline run meltano
dbt build --select staging silver gold

cd ../weather
source <(uv run celine-utils pipeline run envs)
dbt build
```

The interesting failure here is a provider changing a column in its `weather__*` model.
`weather` reads the alias, so the break surfaces in the facade, one pipeline away from the
edit.

### Chain 3 — grid

```text
om (wind, heat)  →  om_wind_gusts, om_heat_risk  ─┐
[private CIM ingestion]  →  silver grid topology ─┴→  grid
```

```bash
cd apps/om
source <(uv run celine-utils pipeline run envs)
uv run celine-utils pipeline run prefect -f pipeline_wind
uv run celine-utils pipeline run prefect -f pipeline_heat
dbt test --select "tag:wind tag:heat"

cd ../grid
source <(uv run celine-utils pipeline run envs)
dbt build
```

Requires PostGIS and the private silver topology tables. A missing-column error on
`strike_tree_*` is an upstream staleness, not a grid regression.

### Chain 4 — photovoltaic

```text
overture  →  pv_overture_buildings  ─┬→  pv_estimation  (also reads rec_it gold)
                                     └→  pv_detection
trentino_rooftops  →  pv_building_suitability
```

```bash
cd apps/overture
source <(uv run celine-utils pipeline run envs)
uv run celine-utils pipeline run meltano
dbt build

cd ../trentino_rooftops && source <(uv run celine-utils pipeline run envs) && dbt build
cd ../pv_estimation   && source <(uv run celine-utils pipeline run envs) && dbt build
cd ../pv_detection    && source <(uv run celine-utils pipeline run envs) && dbt build
```

`pv_detection`'s raw layer needs a vision-model endpoint; run the dbt layers against
whatever `raw.pv_predictions` already holds rather than re-inferring.

## Current coverage

Honest state, so a gap is a known gap rather than a surprise. Counts are of declared dbt
tests; regenerate with `dbt ls --resource-type test` inside an app.

| App | dbt generic | dbt singular | dbt unit | pytest |
|---|---|---|---|---|
| `om` | yes | — | — | — |
| `rec_flexibility` \* | yes | 9 | yes | yes |
| `mt` | yes | 5 | — | — |
| `grid` | yes | — | — | — |
| `rec_metering` | yes | 3 | — | — |
| `weather` | yes | 3 | — | — |
| `rec_it` | yes | — | — | — |
| `pv_estimation`, `pv_detection` | source-level only | — | — | — |
| `copernicus`, `dwd`, `osm`, `overture`, `owm`, `trentino_rooftops` | **none** | — | — | — |
| `rec_registry`, `rec_flexibility_commitments` | no dbt project | — | — | — |

\* `rec_flexibility` has by far the largest suite in the repository, and **none of the dbt
half runs on a fresh checkout** until the private fleet seed is generated — see the
cascade above. Concluding it is untested is the usual mistake.

The unstarted apps are the backlog, roughly in value order: `owm` (19 models, and the
second weather-contract producer), then the geospatial three, then `copernicus`.

## Reporting

Name the layers that ran, the layers that did not, and why. A layer skipped because it
needs infrastructure you did not start is a fact about the evidence, not an admission.

**A green run is only evidence about what actually ran.** An incremental model tested
against an empty table passes every test it has. When a suite reports success on a
pipeline whose upstream you could not provide, say that in the same sentence as the pass.
