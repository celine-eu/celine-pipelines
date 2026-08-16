# rec_metering

Promotes normalized 15-minute meter readings from the silver layer into gold tables consumed by all other REC pipelines.

## Upstream dependency

Reads from a single silver table produced by a metering ingestion pipeline (not included in this repository). The expected schema is declared in `dbt/models/gold/sources.yml`.

**Schema resolution:** the source schema is read from the `CELINE_SILVER_SCHEMA` env var (default: `ds_dev_silver`). Set this in `.env` to match your deployment.

**Providing the table:** the upstream pipeline must materialise `meters_data_normalized` into the configured schema before this pipeline runs. This is typically a private pipeline that normalises smart-meter readings from the local metering infrastructure. For local development, create the table manually with DDL matching the columns below and load sample data.

`meters_data_normalized` must expose the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `device_id` | text | Device identifier (matches `sensor_id` in the REC registry) |
| `ts` | timestamptz | 15-minute interval start, aligned to a quarter-hour boundary |
| `consumption_kwh` | numeric | Grid import over the interval (**kWh per 15-min bucket**) |
| `production_kwh` | numeric | Grid export over the interval (kWh per 15-min bucket) |
| `self_consumed_kwh` | numeric | Behind-the-meter self-use (kWh per bucket), **unclipped** — negative on a minority of rows upstream |

These are energy quantities per bucket, not instantaneous power. There is no kW→kWh
conversion anywhere in this pipeline or downstream of it.

## dbt models

### `meters_data_15m`

Deduplicates and aggregates normalized 15-min readings per `(device_id, ts)`. Groups by slot to collapse any duplicate readings from the source. Incremental merge on `md5(device_id || ts)`.

**Outputs** (`ds_dev_gold.meters_data_15m`):

| Column | Description |
|--------|-------------|
| `_id` | `md5(device_id \|\| ts)` |
| `device_id` | |
| `ts` | 15-min slot start |
| `consumption_kwh` | Summed across source rows for the slot |
| `production_kwh` | Summed across source rows for the slot |
| `self_consumed_kwh` | Summed across source rows for the slot |

### `meters_data_1h`

Hourly rollup of `meters_data_15m` via `date_trunc('hour', ts)`. Incremental merge on `md5(device_id || date_trunc('hour', ts))`.

**Outputs** (`ds_dev_gold.meters_data_1h`):

| Column | Description |
|--------|-------------|
| `_id` | `md5(device_id \|\| ts_hour)` |
| `device_id` | |
| `ts` | Hour start |
| `consumption_kwh` | Sum of 15-min readings in the hour |
| `production_kwh` | Sum of 15-min readings in the hour |

### `meters_data_15m_missing_intervals`

Quality model. Generates the complete expected 15-min grid per device over the last 7 days, then left-joins against actuals to surface gaps. Incremental merge on `md5(device_id || expected_ts)`.

**Outputs** (`ds_dev_gold.meters_data_15m_missing_intervals`):

| Column | Description |
|--------|-------------|
| `event_id` | `md5(device_id \|\| ts)` |
| `device_id` | |
| `ts` | Missing interval timestamp |
| `hour` | Hour of day (Europe/Rome) |
| `created_at` | Detection timestamp |

## Flow (`flows/pipeline.py`)

`rec-metering-flow` runs two tasks in sequence: **Transform Gold Layer** (`dbt run --select gold`) followed by **Run dbt Tests**. Serves with cron `*/10 * * * *` (every 10 minutes) in dev mode.
