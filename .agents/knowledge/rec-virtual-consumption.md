# REC virtual consumption — what the numbers mean, and what is wrong with them

Verified 2026-07-30; the governance and model claims re-checked against the code 2026-08-14.

## The community spans three primary substations, not one

Seventeen monitored members across three `cabine primarie`. `raw.rec_registry_mirror.topology_ids`
always has cardinality one today, so indexing `silver_rec_registry.topology_ids[1]` is safe — but
that is a property of the current data, not a guarantee.

**Per-substation netting in `apps/rec_it/dbt/models/gold/rec_virtual_consumption_15m.sql` is
correct and must not be "simplified".** Sharing cannot cross an unconnected cabina primaria;
netting community-wide would invent energy that physically cannot flow. The community figure is
`least()` per `(ts, rec_id, substation_id)` **and then** summed. Anyone reading the model as
needlessly complicated is about to introduce energy that does not exist.

## The hourly model is wrong, and fixing it alone changes nothing

`rec_virtual_consumption_hourly.sql` computes `sum(self_consumption_kwh)`, that is
`Σ least(i₁₅, e₁₅)`. The hourly definition needs `least(Σ i, Σ e)`. Since `Σ min ≤ min Σ`, it can
only understate. Measured gap over 90 days of real data: **+3.9%** (1594.71 → 1657.07 kWh).

It has **no consumer** — every application fetcher reads the 15-minute model. So correcting the
hourly model in isolation moves nothing visible, and any change to community netting must move
`rec_virtual_consumption_per_device_15m` with it: that model allocates the 15-minute pool
pro-rata, and the invariant that member shares sum to the community total breaks otherwise.

## The column names mean something other than they say

| Column | Actually is |
|---|---|
| `total_consumption_kwh` | community **grid import** (prelievo) |
| `total_production_kwh` | **grid export** (immissione), prosumers only |
| `self_consumption_kwh` | **collective shared** energy, not individual PV self-use |

`schema.yml` carries no column descriptions for any of the four `rec_virtual_consumption_*`
models, so the names are the only documentation and they mislead.

`self_consumption_ratio` is **not summable** — it is per-substation with production as
denominator. The webapp ignores it and recomputes using *consumption* as denominator. Two
different KPIs share one name; check which one a consumer means before comparing figures.

## The community models have no row filter

Confirmed at `apps/rec_it/governance.yaml`: `rec_virtual_consumption_15m` (line 48) and
`rec_virtual_consumption_hourly` (line 70) declare **no `row_filters`**, while
`rec_virtual_consumption_per_device_15m` (77) and `_per_device_hourly` (102) both carry the
`rec_registry` handler.

Nothing enforces REC scoping on the community models downstream. Harmless while one REC exists;
a cross-community leak the moment a second one does. This is latent rather than theoretical —
it needs no code change to become live, only a second REC.

## Upstream metering

The chain reaching this repository ends at `rec_metering.meters_data_15m`, a pass-through into
the `rec_it` models. Everything above it lives in the **private** deployment repository, which
matters when writing here: this repository is public, so upstream internals — paths, commits,
bucket layout — do not belong in these notes.

Two properties of that boundary are worth knowing:

- `meters_data_15m.ts` is exactly quarter-aligned (`:00/:15/:30/:45`, second zero), which is what
  lets cross-device `least()` net genuinely coincident energy.
- Upstream `self_consumed_kwh` is **unclipped and negative on 12.2% of rows** (44,603 of 364,401,
  worst −4.665 kWh), because it computes a difference with no `max(..., 0)`. The flexibility app
  clips it in its own silver layer and has a dbt test pinning that. It does **not** affect
  autoconsumo: `rec_virtual_consumption_15m` reads only `consumption_kwh` and `production_kwh`.
