# `rec_flexibility` cannot be parsed or run from a fresh checkout

Verified 2026-08-15.

`apps/rec_flexibility/dbt/seeds/rec_active_devices.csv` is **not in the repository and
never will be** — it holds the private device fleet, and this repository is open source.
`flows/pipeline.py` writes it at flow start from `REC_ACTIVE_DEVICES` (or the empty
`fleet.active_devices` fallback in `flexibility_config.yaml`) before any dbt task runs.

So on a fresh checkout, every bare dbt command in this app fails at parse time:

```text
Compilation Error
  Model 'model.rec_flexibility.rec_meters_15m' depends on a node named
  'rec_active_devices' which was not found
```

That is not a missing dependency to hunt down and not a broken model. Generate the seed
first — from the app directory:

```bash
uv run python -c "
import sys; sys.path.insert(0, '.')
from pathlib import Path
from lib.config import get_active_devices, load_config, write_active_devices_seed
print(write_active_devices_seed(get_active_devices(load_config()), Path('dbt/seeds/rec_active_devices.csv')))
"
```

Running the flow through `celine-utils pipeline run prefect` does this for you; running
`dbt` directly does not.

## An empty fleet parses but does not run

`write_active_devices_seed` documents the empty case as "dbt still parses; scope resolves
to none". Parsing is where that stops being true. With no `REC_ACTIVE_DEVICES` set the
file is header-only, dbt-postgres has no values to infer from and types `device_id` as
`integer`, and `rec_meters_15m` then fails at execution:

```text
Database Error in model rec_meters_15m
  operator does not exist: text = integer
  LINE 42: where m.device_id in (select device_id from active)
```

`dbt_project.yml` declares no `seeds:` block, so nothing pins the column type. Local work
on this app therefore needs a non-empty fleet: set `REC_ACTIVE_DEVICES` to device ids that
exist in `ds_dev_gold.meters_data_15m`, or add `fleet.active_devices` to a local config
copy. **Do not commit either.**

## What this costs

The app's own test suite is its largest — 82 declared dbt tests, a `unit_tests.yml` of
over a thousand lines, and a pytest suite — and none of the dbt half can run until the
seed exists. Anyone concluding "rec_flexibility has no runnable tests" has hit this and
stopped one step early.

A related, separate constraint: **dbt unit tests need the model's relation to exist**,
because dbt reads the column types off it to build the fixture. `dbt test --select
test_type:unit` on a project whose models have never been built errors with
`Not able to get columns for unit test ... because the relation doesn't exist`, which
looks like a broken test and is a missing `dbt run`.
