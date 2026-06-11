# rec_flexibility

Computes flexibility opportunity windows, per-commitment settlement with proportional redistribution, three-layer gamification scoring (settlement points + flexibility bonus + commitment mechanism), anti-gaming flags, and CO2 impact for the REC flexibility programme.

## Sources

| Table | Schema | Origin | Description |
|-------|--------|--------|-------------|
| `meters_data` | `ds_dev_silver` | private ingestion | 15-min raw meters, one row per `(device, ts, meter_type ∈ {M1, M2, M2_2})`. Columns `consumption_kw` / `production_kw` are **kW** — multiply by 0.25 for 15-min kWh. |
| `meters_energy_forecast` | `ds_dev_gold` | meter_forecasting pipeline | Per-device 48h energy forecast |
| `total_meters_forecast` | `ds_dev_gold` | meter_forecasting pipeline | Community-level hourly net-exchange forecast |
| `flexibility_commitments_mirror` | `raw` | private ingestion | 90-day sliding mirror from the Flexibility API, refreshed every 15 min |

**Schema resolution:** `ds_dev_silver` and `ds_dev_gold` are read from the `CELINE_SILVER_SCHEMA` and `CELINE_GOLD_SCHEMA` env vars. Set these in `.env` to match your deployment. The `raw` schema is fixed.

**Providing private upstream tables:**

- **`meters_data`** — silver-layer meter readings produced by a private metering ingestion pipeline. Must expose `device_id`, `ts`, `meter_type` (M1/M2/M2_2), `consumption_kw`, `production_kw`. For local development, create the table manually and load sample data.
- **`meters_energy_forecast` / `total_meters_forecast`** — produced by the meter_forecasting pipeline (not in this repository). If unavailable, the flexibility windows model will produce no output (no surplus windows detected). For local development, populate with synthetic forecast rows.
- **`flexibility_commitments_mirror`** — raw mirror of the Flexibility API (`flexibility-api` service). Loaded by a private pipeline that periodically syncs the API into `raw.flexibility_commitments_mirror`. Must expose `commitment_id`, `device_id`, `status`, `period_start`, `period_end`, `last_updated`. For local development, create the table manually.

The full source contracts are declared in `dbt/models/silver/sources.yml` and `dbt/models/gold/sources.yml`.

> **Note:** `ds_dev_gold.meters_data_15m` and `ds_dev_gold.meters_data_1h` are **banned**. `_1h` sums instead of averaging the four 15-min kW values (4× inflated); `_15m` is not allowed by project rule. Every downstream model reads the silver intermediate `rec_meters_15m` instead (see below).

### Seed

`co2_factors.csv` — country CO2 intensity (kg/kWh) and trees-per-ton conversion for 8 EU countries. Selected at runtime via dbt var `co2_country` (default: `it`).

## dbt models

### Silver

#### `silver_flexibility_commitments`

Normalises the raw API mirror: casts all timestamps to `timestamptz`, computes `window_duration_hours`, and strips any `StatusSchema.` enum-repr prefix from `status` so downstream bare-token filters (`status in ('committed','settled')`) match. One row per commitment. Full rebuild on each run.

#### `rec_meters_15m`

Analysis-ready 15-min meter table derived from `ds_dev_silver.meters_data`. Pivots M1 against combined M2/M2_2 per `(device, ts)`; derives `self_consumed_kw = clip(pv_production_kw − grid_export_kw, ≥0)`; exposes `consumption_kw`, `production_kw (= grid_export_kw)`, `pv_production_kw`, `self_consumed_kw`, `total_consumption_kw`. Values remain in kW — downstream multiplies by 0.25 for 15-min kWh. Mirrors the pattern used in `src/notebooks/gamification/data_loader.py::load_meters`. Materialized as a view.

### Gold

#### `rec_flexibility_windows`

Pre-computed daily load-shift windows derived from community solar surplus in `total_meters_forecast`. Logic:

1. Identifies hours where `net_exchange_kwh > 0.5` (surplus threshold), skipping 00:00–05:00.
2. Groups consecutive surplus hours per calendar day; caps each group at 3 hours (`MAX_WINDOW_HOURS`); drops windows shorter than 2 hours (`MIN_WINDOW_HOURS`).
3. Cross-joins with per-device consumption forecasts from `meters_energy_forecast` to compute `estimated_kwh` per device per window.
4. Normalises `estimated_kwh` proportionally against `community_kwh` so the sum across all devices for a window never exceeds the available solar surplus.

`reward_points_estimated = round(estimated_kwh × 10)`. `flexibility_model` tag is set via dbt var (default: `solar_overproduction`). Incremental: refreshes today and tomorrow on every run.

#### `rec_settlement_15m`

Every 15-min metered interval for every device, annotated with flexibility window context. Left-joins `rec_meters_15m` (silver-derived) against `rec_flexibility_windows`; `window_start IS NOT NULL` marks intervals inside a window. `DISTINCT ON (device_id, ts)` keeps the earliest matching window in case of overlap. `consumption_kwh = consumption_kw × 0.25` (unit-correct kW → 15-min kWh).

#### `rec_settlement_1h`

Hourly rollup of `rec_settlement_15m`. Carries forward `window_start`, `window_end`, and `window_estimated_kwh` via `MAX` aggregation. Consumed by `rec_commitment_settlement` and by the Flexibility API for querying actual consumption within a committed window.

#### `rec_commitment_settlement`

Per-commitment settlement with proportional redistribution of the window budget.

**Committed devices have priority access to the full `community_kwh` budget.** Passive (non-committed) device consumption is not counted against this budget here.

- **`actual_kwh`** — raw device consumption during the committed period (from `rec_settlement_1h` where `window_start IS NOT NULL`).
- **`allocated_kwh`** — the device's share of the community budget, computed as follows:

  | Condition | allocated_kwh |
  |-----------|--------------|
  | `status` is rejected or cancelled | 0 |
  | No matching window budget row (DSO flex or window outside refresh range) | `actual_kwh` (passthrough) |
  | `sum(actual_kwh for committed devices in window)` ≤ `community_kwh` | `actual_kwh` (no adjustment needed) |
  | `sum(actual_kwh for committed devices in window)` > `community_kwh` | `actual_kwh / total_actual × community_kwh` (proportional share) |

- **`reward_points_actual`** — sum of the device's Layer-1 settlement points over `[period_start, period_end)` (from `rec_settlement_points`) **+** the device's Layer-2 flexibility bonus for the matching window (from `rec_flexibility_bonus`). Authoritative over the API mirror. Supersedes the prior linear `round(allocated_kwh × 10)`.
- **`adherence_ratio`** — `reward_points_actual / reward_points_estimated` for settled commitments only. Both sides use the same log-scaled formula (`rec_flexibility_windows.reward_points_estimated` was rewritten to log-scale in this migration), so the ratio remains a coherent delivery measure. NULL while status is still `committed`.

All statuses (committed, settled, rejected, cancelled) are included for acceptance-rate and no-show analytics. Incremental merge on `commitment_id`, refreshing commitments updated in the last 2 days.

#### `rec_gamification_summary`

Per-device daily ranking and budget-aware passive points allocation.

**Two-tier points system:**

| Tier | Who | `total_allocated_kwh` | Points source |
|------|-----|-----------------------|---------------|
| Committed | Device held a `committed`/`settled` status on that date | 0 (excluded from passive pool) | `rec_commitment_settlement.reward_points_actual` via the Flexibility API |
| Passive | Device consumed during a window but held no active commitment | Proportional share of `remaining_budget` | `rec_participant_points.daily_points` |

**Passive allocation per window:**

```
committed_allocated_total = sum(allocated_kwh from rec_commitment_settlement for that window)
remaining_budget          = max(0, community_kwh − committed_allocated_total)

if remaining_budget = 0:
    passive allocated_kwh = 0            (committed devices used all solar surplus)
elif passive_total ≤ remaining_budget:
    passive allocated_kwh = actual_kwh   (all passive consumption fits in budget)
else:
    passive allocated_kwh = actual_kwh / passive_total × remaining_budget
```

`total_consumption_kwh` (raw window consumption) is used for the daily leaderboard ranking (`percentile_rank`, `rank_position`) across all devices regardless of committed status.

#### `rec_settlement_points` *(new)*

Per-15-min settlement points using **log scaling + effort multiplier**. Layer-1 building block of the scoring system.

- **Surplus interval** (`community_prod_kwh ≥ community_cons_kwh`): `settlement_points = ln(1 + consumption_kwh) × effort_multiplier × 10`. Effort tiers (from `flexibility_config.yaml`): 0.25× below baseline, 0.50× at-to-1.10× baseline, 1.00× at 1.10×, 1.50× at 1.25×, 2.00× at 1.50×.
- **Deficit-with-production** (`0 < community_prod_kwh < community_cons_kwh`): all devices share a linear pool (`community_prod_kwh × 10`) weighted by `ln(1 + consumption_kwh) × effort_multiplier`.
- **No production**: 0 points.

Every device, every interval. Baseline comes from `rec_device_baselines` (Python-written).

#### `rec_flexibility_bonus` *(new)*

Per-window bonus — **only for committed devices** (semi-join against `silver_flexibility_commitments` where `status in ('committed','settled')`).

`bonus = shifted_kwh × 15 × shift_effort_mult × event_mult × accuracy × streak_mult`, capped per-window at `2 × device's 30-day rolling avg`. `shifted_kwh = actual_kwh − reference_baseline_kwh` (reference baseline integrated across every 15-min slot in `[window_start, window_end)`). Fires only when `shift_fraction ≥ 5%` AND `shifted_kwh ≥ 0.05 kWh`.

#### `rec_anti_gaming_flags` *(new)*

L3 (per-window bonus exceeded the 2× rolling-avg cap) and L4 (daily bonus > 10× 30-day median) flags. L1/L2/L5 are not implemented at the current 10-device scale — threshold-based layers are the only ones with statistical power.

#### `rec_device_baselines` *(new, view)*

Thin view over `_rec_device_baselines_raw`. Two baseline types: `settlement` (rolling High 4 of 7, refreshed daily) and `reference` (winsorized High 4 of 7 over 90 days, refreshed on the 1st of each month). `baseline_kwh` is kWh per 15-min bucket. Written by `compute_baselines_task`.

#### `rec_device_streaks` *(new, view)*

Thin view over `_rec_device_streaks_raw`. One row per device with current `level`, historical `peak`, and the derived `multiplier` (`1.0 + level × 0.05`, capped at `1.5`). Written weekly by `update_streaks_task`.

#### `rec_participant_points`

Per-device daily points — three-layer scoring (2026-04-17):

```
daily_settlement_points = sum(rec_settlement_points.settlement_points) over the day  # every device
daily_bonus_points       = sum(rec_flexibility_bonus.bonus_points) over the day       # committed only
daily_points             = daily_settlement_points + daily_bonus_points
```

Supersedes the prior `round(total_allocated_kwh × 10)` formula. `daily_consumption_kwh` carries raw window consumption for display.

#### `rec_co2_savings`

Per-device daily CO2 avoided from all self-consumed renewable energy (not limited to flexibility windows). `co2_avoided_kg = consumption_kwh × kg_co2_per_kwh`. Country factor from `co2_factors` seed via `co2_country` var.

#### `rec_co2_savings_community`

Community-level daily CO2 avoided scoped to intervals inside flexibility windows only (`window_start IS NOT NULL`), attributing impact to the flexibility programme specifically. Includes `participating_devices` count.

## Tests

### Schema tests (`dbt test --select tag:rec_flexibility`)

`unique` and `not_null` on all surrogate keys and key dimension columns across all gold models. Defined in `models/gold/schema.yml`.

### Unit tests (`dbt test --select tag:rec_flexibility`)

Defined in `models/gold/unit_tests.yml`. Cover 21 scenarios across four models:

| Model | Tests |
|-------|-------|
| `rec_flexibility_windows` | W1–W8: window grouping, gap detection, sleeping hours, sub-window capping, proportional normalisation |
| `rec_commitment_settlement` | C1–C6: under/over budget allocation, rejected status, DSO fallback, adherence ratio |
| `rec_settlement_15m` | S1–S4: window join, LEFT JOIN preservation, kW→kWh conversion, overlapping window deduplication |
| `rec_gamification_summary` | G1–G3: passive budget share, budget exhausted by committed, passive proportional cap |

### Singular tests (`dbt test --select tag:rec_flexibility`)

Defined in `tests/`:

- `commitment_allocation_within_budget` — sum of committed `allocated_kwh` per window must not exceed `community_kwh`.
- `passive_allocation_within_remaining_budget` — passive `total_allocated_kwh` per day must not exceed the remaining budget after committed allocation.

## Flow (`flows/pipeline.py`)

`rec-flexibility-flow` runs five tasks in sequence:

1. **Seed dbt** (`dbt seed`) — seeds `co2_factors.csv`.
2. **Compute Baselines** (`compute_baselines_task`) — reads `ds_dev_silver.meters_data` via `lib/meters.py`, runs High 4/7 settlement + winsorized reference baseline (`lib/baselines.py`), writes `ds_dev_gold._rec_device_baselines_raw`.
3. **Update Streaks** (`update_streaks_task`) — reads previous state + the past week of `rec_flexibility_bonus`, applies one weekly decay step (`lib/streaks.py`), writes `ds_dev_gold._rec_device_streaks_raw`.
4. **Transform Gold Layer** (`dbt run --select gold`).
5. **Run dbt Tests** (`dbt test`).

Serves with cron `*/15 * * * *` (every 15 minutes) in dev mode. The Python tasks are idempotent (DELETE+INSERT inside a single `engine.begin()` transaction).

## Configuration

All tunable parameters live in `flexibility_config.yaml` (shared with `src/notebooks/gamification/`). Edit the YAML, not the code, to retune baselines, effort tiers, bonus multipliers, streak decay, and anti-gaming thresholds.
