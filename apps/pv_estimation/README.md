# PV Estimation Pipeline

Estimates the return on investment (ROI) of photovoltaic installations on
rooftops in Trentino. Uses the `celine-roi` library to compute financial
metrics for each eligible building.

Includes a Streamlit dashboard for policy makers and REC managers to evaluate
investment at province and primary substation level.


## Quick start

    cd apps/pv_estimation

    task estimate                   # compute ROI for all buildings (incremental)
    task dbt:seed                   # load adoption rate parameters
    task dbt:run                    # build dbt models (staging → gold + REC)
    task dashboard                  # launch the Streamlit dashboard

Full refresh (recompute everything):

    task run:full-refresh


## Task reference

    task estimate                   Parallel ROI estimation (incremental)
    task estimate:full-refresh      Truncate and recompute all buildings
    task estimate:dry-run           Test on 10 buildings without writing to DB

    task dbt:deps                   Install dbt packages
    task dbt:seed                   Load seed data (adoption rate curve)
    task dbt:run                    Run all dbt models
    task dbt:gold                   Run gold models only
    task dbt:rec                    Run REC substation models only
    task dbt:full-refresh           Full refresh all dbt models
    task dbt:test                   Run dbt tests

    task run                        Full pipeline: estimate + seed + dbt
    task run:full-refresh           Full refresh of everything

    task dashboard                  Launch Streamlit dashboard
    task serve                      Start Prefect flow server (dev mode)

Pass extra arguments with `--`:

    task estimate -- --limit 5000 --workers 4
    task estimate:full-refresh -- --batch-size 2000


## Data flow

This pipeline sits downstream of three other pipelines:

    overture → trentino_rooftops → pv_detection → pv_estimation

1. **overture** ingests Overture Maps building footprints and produces a silver
   table with geometry, area and building metadata.

2. **trentino_rooftops** filters buildings against regulatory constraints
   (aree non idonee, vincoli diretti/indiretti) to produce a gold table of
   suitable rooftops.

3. **pv_detection** runs a vision model on aerial imagery to flag which
   buildings already have PV panels installed.

4. **pv_estimation** (this pipeline) takes suitable buildings and estimates
   the financial return of installing panels.

The pipeline has no meltano extraction step. The Prefect flow reads directly
from the upstream gold/silver tables, computes ROI via celine-roi, and writes
results to a raw table. dbt then transforms through staging, silver and gold.


## Eligible buildings

Controlled by `require_detection` in config.yaml:

- `require_detection: true` — only buildings scanned by pv_detection and
  confirmed as `has_pv = false`. Conservative, waits for detection coverage.
- `require_detection: false` — all buildings from `pv_building_suitability`.
  Useful for full-coverage projections before detection is complete.

In both cases, buildings must pass the regulatory suitability filter from
trentino_rooftops and meet the minimum system size (min_kwp, default 3 kWp).


## Building classification

Overture Maps building_class is mostly NULL, so the pipeline uses a heuristic
to classify buildings based on footprint area and floor count:

    if Overture building_class is present:
        use the explicit class mapping from config

    else if num_floors > 3:
        → commercial (likely apartment block or office)

    else if footprint_area_m2 > 200:
        if area >= 500 and floors <= 1:
            → industrial (large single-story: warehouse, factory, barn)
        else:
            → commercial

    else:
        → residential

The thresholds are configurable under `estimation.classification` in
config.yaml. The classification drives both the consumption model and the
celine-roi user_type (which affects tax treatment and incentive eligibility).

Future improvements: cross-reference with OSM building tags (already in the
osm pipeline) or cadastral data from Provincia di Trento for more accurate
classification.


## Estimation logic

For each building the pipeline estimates input parameters from the available
data (geometry, footprint area, floor count) and calls `celine-roi` to compute
the financial outcome.

### PV capacity (kWp)

System size is the minimum of three constraints:

    rooftop_kwp = footprint_area_m2 * panel_kwp_per_m2
    kwp = min(rooftop_kwp, consumption_kwp, max_kwp)

Where `panel_kwp_per_m2` is 0.130 kWp/m2, derived from:

- 65% usable roof fraction (excluding edges, obstructions, orientation losses)
- 200 Wp/m2 panel density (standard monocrystalline modules)

**Residential sizing**: the system is sized to match the household's actual
consumption plus a headroom factor (default 20%), not to fill the entire roof:

    consumption_kwp = consumption_kwh * (1 + headroom) / specific_yield

A household consuming 3,500 kWh/year gets a system of ~3.8 kWp (producing
~4,200 kWh), regardless of whether the roof could fit 20 kWp. This reflects
real homeowner behavior — over-building a system that mostly exports to the
grid at low RID tariffs is a poor investment.

**Non-residential**: the system fills the available rooftop up to max_kwp,
since commercial/industrial buildings typically have high enough consumption
to absorb the production.

The result is capped at `max_kwp` (default 20 kWp). This matches the Italian
IRPEF tax deduction threshold — systems above 20 kWp do not qualify for the
50% deduction.

Buildings with estimated kwp below `min_kwp` (default 3 kWp, roughly 6 panels)
are skipped. Below this size the fixed costs (inverter, permitting,
scaffolding, electrician) make the installation uneconomical.

### Annual production (kWh)

    annual_production_kwh = kwp * specific_yield

Where `specific_yield` is 1100 kWh/kWp/year, a conservative estimate for
Trentino based on average solar irradiance at typical tilt angles.

This value is passed directly to celine-roi, bypassing the PVGIS API call.
This avoids rate limiting when processing hundreds of thousands of buildings
and is acceptable for a screening-level estimate.

### Annual consumption (kWh)

Two different models depending on building type:

**Residential** (fixed per household):

    annual_consumption_kwh = 3500  (configurable)

A typical Italian household consumes 2700-4500 kWh/year (ARERA data).
The default of 3500 kWh represents a mid-range family with modern appliances.
This is a fixed value per household, NOT proportional to footprint area —
a 60 m2 apartment and a 200 m2 villa have similar electricity consumption.

**Non-residential** (area-based):

    annual_consumption_kwh = footprint_area_m2 * consumption_per_m2

Where consumption_per_m2 depends on user_type:

    office:       80 kWh/m2/year
    commercial:  100 kWh/m2/year
    industrial:  150 kWh/m2/year
    agricultural: 30 kWh/m2/year

For non-residential buildings, energy consumption does scale with building
size (lighting, HVAC, machinery, refrigeration), so the area-based model
is appropriate.

### CAPEX (EUR)

    capex = kwp * capex_per_kwp

Where `capex_per_kwp` is 1500 EUR/kWp. This is a mid-range estimate for
residential systems under 20 kWp including panels, inverter, mounting and
installation. It does not include VAT (celine-roi expects net-of-IVA values).

### Incentive regime

Configured as `regime` in config.yaml (default: RID). Options:

- RID: feed-in tariff only (Ritiro Dedicato)
- CER: community energy sharing incentives only
- RID_CER: both combined


## Output metrics

For each building, celine-roi returns:

- **npv**: net present value over 25 years at 5.5% discount rate (EUR)
- **irr**: internal rate of return
- **payback_simple**: years to recover investment from gross cash flows
- **payback_discounted**: years to recover investment from discounted cash flows
- **tasso_autoconsumo**: fraction of production consumed on-site (0-1)


## Installation plan (dbt gold models)

On top of per-building estimates, dbt produces a 5-year installation plan:

**pv_installation_ranking** ranks all positive-NPV buildings by payback period
(best first) and computes each building's cumulative percentile position.

**pv_installation_plan** assigns each building to an installation year using
an adoption rate curve defined in the seed file `data/installation_plan_params.csv`:

    year  cumulative_adoption_pct  degradation_rate
    1     2.0                      0.0045
    2     6.0                      0.0045
    3     12.0                     0.0045
    4     20.0                     0.0045
    5     30.0                     0.0045

The best-return buildings are installed first. To change the rollout pace,
edit the CSV and re-run `task dbt:seed && task dbt:gold`.

**pv_installation_plan_summary** aggregates per year with cumulative totals:
new and cumulative buildings, kWp/MWp, production MWh (with degradation),
investment EUR/MEUR, coverage %, weighted IRR, average payback.


## REC substation analysis (dbt gold/rec_it)

Italian RECs operate under GSE primary substation (cabina primaria) coverage
areas. The `rec_it` dbt models break down the estimation by substation:

**rec_building_cabina** spatially joins each building to its primary
substation using `gse_cabine_primarie` geometry from the rec_it pipeline.
Requires GIST indexes on both geometry columns for performance.

**rec_cabina_opportunities** joins the building-cabina mapping with the
per-building ROI estimates.

**rec_cabina_summary** aggregates per substation: total buildings, capacity,
production, consumption, self-consumption, grid export, investment, NPV,
weighted IRR, payback, and building type breakdown.

**rec_cabina_plan** breaks down the 5-year installation plan by substation
with per-year and cumulative totals.

Build with `task dbt:rec` after the main gold models are up.


## Dashboard

The Streamlit dashboard (`tools/dashboard.py`) provides two views:

**Province Overview** — aggregate PV potential across Trentino:
- Total KPIs: buildings, MWp, GWh/year, investment, NPV, IRR, payback
- 5-year installation plan with capacity and investment charts
- Building type breakdown (residential/commercial/industrial)
- Financial distributions: IRR, payback, NPV, self-consumption
- Energy balance: self-consumed vs grid export per yearly cohort

**REC Substation Analysis** — per-cabina primaria investment view:
- Multi-select substations to scope a REC investment area
- Community investment summary: total potential and aggregate NPV
- ROI breakdown with explicit return on investment percentage
- Revenue streams: self-consumption savings vs RID export revenue
- Per-substation detail table with ROI % column
- 5-year plan with cumulative NPV and ROI tracking
- Side-by-side substation comparison

Launch with `task dashboard`.


## Known limitations

- Building classification relies on a simple area + floors heuristic.
  Overture Maps building_class is mostly NULL and num_floors is often missing.
  Misclassified buildings get wrong consumption estimates and tax treatment.
  Cross-referencing with OSM tags or cadastral data would improve accuracy.

- The 20 kWp cap applies to all buildings. Large commercial/industrial
  rooftops could host bigger systems with different economics (no IRPEF
  deduction, IRES/IRAP tax treatment, lower EUR/kWp). A future version
  could model these separately.

- Residential consumption uses a single fixed value. Real consumption varies
  widely based on occupancy, appliances, heating, and building efficiency.
  Heat pump adoption (5000-8000 kWh/year) is not modeled.

- Production uses a flat regional specific yield. Per-building estimates via
  PVGIS or the Trentino Solar API would account for local shading, orientation
  and tilt but are too slow for batch processing.

- CAPEX is flat per kWp. Real costs decrease with system size (economies of
  scale) and vary by installer, roof type, and mounting system.


## Configuration

All estimation parameters are in `flows/config.yaml`. Key values:

    estimation:
      specific_yield: 1100       # kWh/kWp/year
      panel_kwp_per_m2: 0.130    # kWp per m2 of footprint
      max_kwp: 20                # residential cap (IRPEF threshold)
      min_kwp: 3                 # minimum viable system
      capex_per_kwp: 1500        # EUR/kWp net of IVA
      regime: RID                # RID, CER, or RID_CER

      residential_consumption_kwh: 3500  # fixed per household
      residential_sizing_headroom: 0.20  # size system to 120% of consumption

      consumption_per_m2:        # area-based, non-residential only
        commercial: 100
        industrial: 150
        office: 80
        agricultural: 30

      classification:            # building type heuristic
        max_residential_area_m2: 200
        max_residential_floors: 3
        industrial_min_area_m2: 500
