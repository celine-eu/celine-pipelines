# A coordinate is not a unique forecast point — the weather contract carries elevation for that reason

Measured 2026-08-15. The gap described here was **closed** the same day; this records why
the elevation column exists, so nobody removes it as redundant.

## What shares a coordinate

`mt_forecast_locations` holds **320 locations at 297 distinct coordinate pairs**. Neither
"duplicate rows" nor "the same place twice" is the right reading. What collides is
elevation.

**28 of the 32 colliding locations are mountain-massif altitude bands** (`venue_type = 5`)
— 7 massifs × 4 bands. Each massif has one representative lat/lon and a separate forecast
location per altitude:

```text
Dolomiti di Brenta - 1500 m   46.208, 10.903
Dolomiti di Brenta - 2000 m   46.208, 10.903
Dolomiti di Brenta - 2500 m   46.208, 10.903
Dolomiti di Brenta - 3000 m   46.208, 10.903
```

This is a **vertical profile at one map point**, and the forecasts differ exactly as they
should:

| Band | Mean forecast temperature |
|---|---|
| 1500 m | 10.92 °C |
| 2000 m | 7.24 °C |
| 2500 m | 3.67 °C |
| 3000 m | 0.82 °C |

A ~6.7 °C/km lapse rate. Nothing is wrong with the data.

**The other 4 are two point pairs:**

- `Povo` (398 m) and `Trento - collina` (398 m) — same coordinate, same elevation, and
  **identical forecast values**. One place under two names. Harmless, and deliberately not
  flagged by any test.
- `Martignano` (374 m) and `Mattarello` (197 m) — same coordinate, different elevation.
  Two genuinely different villages several kilometres apart. This one is an upstream
  coordinate error, and elevation happens to disambiguate it.

## Why the contract carries elevation

`apps/weather` is provider-neutral: it cannot join on `location_id`, because ids are
provider-specific. It matches a seed location to a provider row by distance. So its
effective key is `(lat, lon, elevation_m)`.

Without `elevation_m` the four bands tied at horizontal distance zero and `row_number()`
picked whichever row Postgres returned first — the facade stayed unique while being free
to publish a 3000 m forecast for a valley town, and to change its mind on the next run.
A ~10 °C swing with no change in the data.

## What was changed

**Producers** — `mt_weather_forecast_hourly`, `mt_weather_forecast_daily` and
`mt_weather_current` now project `elevation_m` into the `weather__*` contract tables.
`weather__alerts` does not: alerts are area-based and are assigned by provider, not by
distance.

**Seed** — `weather_locations.csv` gained an `elevation_m` column, typed explicitly in
`dbt_project.yml` because it is mostly blank and dbt would otherwise infer it as text.

**Facade** — the three ranking models order **lexicographically**, not by a combined
metric:

```sql
order by
    (abs(src.lat - loc.lat) + abs(src.lon - loc.lon)),   -- unchanged primary
    abs(src.elevation_m - loc.elevation_m) nulls last,   -- breaks coordinate ties
    src.location_id                                      -- breaks everything else
```

Lexicographic on purpose. Folding elevation into the distance would need a
metres-per-degree exchange rate nothing justifies, and would change which row wins in
cases that were never ambiguous. This ordering is **identical to the old one wherever the
horizontal distance already had a unique minimum**, so it can only change results where
the old one was arbitrary. Verified: `it_folgaria` still resolves to `Folgaria` (1166 m),
unchanged.

`elevation_m` is optional on both sides. `NULLS LAST` means a candidate declaring its
elevation beats one that does not; if neither declares, `location_id` still makes the
result stable. A provider that never adds the column keeps its old behaviour minus the
non-determinism.

Proof the tie now resolves — a seed placed on the Brenta massif coordinate requesting
1500 m:

| Requested | Chosen | Temperature | Rank |
|---|---|---|---|
| 1500 m | **1500 m** | 14 °C | 1 |
| 1500 m | 2000 m | 12 °C | 2 |
| 1500 m | 2500 m | 9 °C | 3 |
| 1500 m | 3000 m | 6 °C | 4 |

## Deploying this change

The two forecast contract tables and the two facade forecast tables are **incremental
merges**. They carry `on_schema_change = 'append_new_columns'`, so the column appears on
the next run — but **existing rows keep `NULL` until their key is merged again**.

**Do not `--full-refresh` them to fix that.** `mt_weather_forecast_hourly` filters
`forecast_at >= now() - interval '1 hour'`; a full refresh rebuilds from that filter alone
and discards every historical row. On a stale database it produces an empty table.

Forecast rows roll forward on their own — each run merges the live window — so the NULLs
age out within a forecast horizon. If a backfill is genuinely needed, update in place from
the location registry rather than rebuilding.

## How it stays fixed

`apps/mt/dbt/tests/mt_weather_contract_rows_the_facade_cannot_distinguish_must_agree.sql`
asserts that contract rows sharing `(lat, lon, elevation_m, forecast_at)` carry the same
forecast — the exact condition under which the facade would be choosing blind. **Error
severity, expected zero**, and it was *not* zero before this change. It is what stops the
elevation column being dropped as redundant.

`not_null` on `elevation_m` in the mt contract schema is the other half.
