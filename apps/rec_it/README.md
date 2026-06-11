# rec_it

Italian CER-specific settlement pipeline. Computes virtual self-consumption allocation per device under Italian GSE rules, and maintains the primary substation reference layer.

## Sources

| Table | Schema | Origin | Description |
|-------|--------|--------|-------------|
| `meters_data_15m` | `ds_dev_gold` | rec_metering pipeline | 15-min metered readings |
| `rec_registry_mirror` | `raw` | private ingestion | REC participant registry: `user_id`, `rec_id`, `role`, `sensor_ids[]`, `topology_ids[]` |
| `gse_cabine_primarie` | `raw` | meltano (self-contained) | GSE primary substation open dataset |

**Schema resolution:** `ds_dev_gold` is read from the `CELINE_GOLD_SCHEMA` env var. Set this in `.env` to match your deployment. The `raw` schema is fixed.

**Providing private upstream tables:**

- **`meters_data_15m`** — produced by the `rec_metering` pipeline in this repository. Run rec_metering first.
- **`rec_registry_mirror`** — a raw table mirroring the REC registry (`rec-registry` service). This is loaded by a private ingestion pipeline that periodically syncs the registry API into `raw.rec_registry_mirror`. For local development, create the table manually with columns `user_id`, `rec_id`, `role`, `sensor_ids` (text[]), `topology_ids` (text[]) and load sample data.
- **`gse_cabine_primarie`** — self-contained: loaded via the included meltano extractor (`tap-copertura-cabine-primarie-gse`). Run `meltano run import` inside the `meltano/` directory.

The full source contracts are declared in `dbt/models/silver/sources.yml` and `dbt/models/gold/sources.yml`.

## dbt models

### Silver

#### `silver_rec_registry`

Unnests `sensor_ids` from the raw registry mirror so downstream models join on individual `sensor_id` without array handling. Takes `topology_ids[1]` as the primary `substation_id`. One row per `(user_id, rec_id, sensor_id)`.

#### `silver_gse_cabine_primarie`

Casts the raw JSON feature bag from the GSE open dataset into typed columns. Filters deleted records (`_sdc_deleted_at is null`). Outputs: `fid`, `geometry`, `cod_ac`, `rag_soc`, `cod_conseg`, `shape__area`, `shape__length`, `last_updated_at`.

### Gold

#### `rec_virtual_consumption_15m`

Community-level virtual self-consumption per `(ts, rec_id, substation_id)`. Only devices registered in `silver_rec_registry` contribute. Production is attributed only to `role='prosumer'` devices.

```
self_consumption_kw = least(total_consumption_kw, total_production_kw)
self_consumption_ratio = self_consumption_kw / total_production_kw
```

Incremental merge on `(ts, rec_id, substation_id)`.

#### `rec_virtual_consumption_hourly`

Hourly rollup of `rec_virtual_consumption_15m` via `date_trunc('hour', ts)`. Recomputes `self_consumption_ratio` from aggregated totals. Incremental merge on `(ts, rec_id, substation_id)`.

#### `rec_virtual_consumption_per_device_15m`

Allocates the community's available self-consumption energy (`available_kwh = self_consumption_kw × 0.25`) to each device proportionally by consumption share:

```
ratio = device_consumption_kwh / total_consumption_kwh
virtual_consumption_kwh = ratio × available_kwh
```

Joins `meters_data_15m` with `silver_rec_registry` and `rec_virtual_consumption_15m`. Incremental merge on `md5(device_id || ts || rec_id || substation_id)`.

#### `rec_virtual_consumption_per_device_hourly`

Hourly rollup of `rec_virtual_consumption_per_device_15m`. Incremental merge on `md5(device_id || ts_hour || rec_id || substation_id)`.

#### `gse_cabine_primarie`

Promotes `silver_gse_cabine_primarie` to the gold schema. Incremental merge on `cod_ac` (updates geometry and attributes when the source dataset changes).

## Flow (`flows/pipeline.py`)

`rec-it-flow` runs two tasks in sequence: **Transform Gold Layer** (`dbt run --select tag:rec_it`) followed by **Run dbt Tests** (`dbt test --select tag:rec_it`). Serves with cron `*/15 * * * *` (every 15 minutes) in dev mode.
