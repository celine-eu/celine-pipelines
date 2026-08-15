# Pipelines Reference

One entry per application under `apps/`. Each app's own `README.md` is the authoritative
detail — column lists, DDL contracts, algorithm notes. This page is the map: what exists,
what it consumes, what it publishes, and when it runs.

Schemas are written with their development defaults (`ds_dev_staging`, `ds_dev_silver`,
`ds_dev_gold`). All cron expressions are UTC.

## Index

| Pipeline | Kind | Schedule | Ingests via |
|---|---|---|---|
| [`om`](#om--open-meteo) | Open data — weather | 4 flows, see below | Meltano |
| [`mt`](#mt--meteotrentino) | Open data — weather | `5 * * * *` | Meltano |
| [`owm`](#owm--openweathermap) | Open data — weather | `2 * * * *` | Meltano |
| [`copernicus`](#copernicus--era5-and-cams) | Open data — climate | `0 0,6,12,18 * * *` | Meltano |
| [`dwd`](#dwd--icon-d2-paused) | Open data — weather | paused | Meltano |
| [`weather`](#weather--multi-provider-facade) | Facade | `15 * * * *` | — (dbt only) |
| [`osm`](#osm--openstreetmap) | Open data — geospatial | `0 0,6,12,18 * * *` | Meltano |
| [`overture`](#overture--overture-maps-buildings) | Open data — geospatial | `0 2 * * *` | Meltano |
| [`trentino_rooftops`](#trentino_rooftops--pv-siting-constraints) | Open data — geospatial | `0 2 * * *` | Meltano |
| [`pv_estimation`](#pv_estimation--rooftop-pv-roi) | Analysis | `0 4 * * 1` | Python + Meltano |
| [`pv_detection`](#pv_detection--existing-pv-from-imagery) | Analysis | `0 3 * * 0` | Python |
| [`grid`](#grid--grid-resilience) | Analysis | `0 8 * * *`, `20/15 * * * *` | — (dbt only) |
| [`rec_registry`](#rec_registry--registry-mirror) | Mirror | `*/5 * * * *` | Python |
| [`rec_flexibility_commitments`](#rec_flexibility_commitments--commitments-mirror) | Mirror | `*/15 * * * *` | Python |
| [`rec_metering`](#rec_metering--metering-interface) | REC | `*/10 * * * *` | — (dbt only) |
| [`rec_it`](#rec_it--italian-cer-settlement) | REC | `*/15 * * * *`, `0 0 * * *` | Meltano |
| [`rec_flexibility`](#rec_flexibility--flexibility-and-gamification) | REC | `0 6 * * *` | — (dbt + Python) |

---

# Weather and climate

## `om` — Open-Meteo

Four independent Prefect flows over the free Open-Meteo API (no API key; CC-BY 4.0),
sharing one image and separated by dbt tag.

| Flow | Cron | Purpose |
|---|---|---|
| `om-weather-daily` | `0 6 * * *` | 7-day hourly forecast, engineered ML features |
| `om-wind-trentino` | `0 0,4,8,12,16,20 * * *` | Wind speed/gust/direction on a 4.4 km Trentino grid (~798 points) |
| `om-heat-trentino` | `30 7 * * *` | Heat risk by altitude band (P90) |
| `om-obs` | `15 1,3,5,7,9,11,13,15,17,19,21,23 * * *` | 15-minute observations |

| Layer | Datasets |
|---|---|
| raw | `raw.om_weather`, `raw.om_weather_wind`, `raw.om_heat`, `raw.om_obs` |
| staging | `stg_om_weather`, `stg_om_wind`, `stg_om_heat`, `stg_om_obs` |
| silver | `om_weather_hourly`, `om_wind_hourly`, `om_heat_daily`, `om_obs_15min` |
| gold | `om_weather_features`, `om_weather_features_meters`, `om_weather_hourly_view`, `om_wind_gusts`, `om_heat_risk` |

`om_weather_features` carries 29 engineered features: temporal and Fourier encodings,
rolling statistics, thermal dynamics and interaction terms, for downstream energy
forecasting. The wind flow uses POST rather than GET against the API to avoid URL-length
limits and per-location rate limiting.

**Downstream:** `apps/grid` (`grid_wind_risks`, `grid_heat_risks` and their nowcasting
variants).

This is the only app in the repository with a `packages.yml` (`dbt_utils`), and the one
with the broadest test coverage — use it as the reference for how a mature app is tested.

## `mt` — MeteoTrentino

Regional weather from the meteorological service of the Autonomous Province of Trento:
station observations, hourly and daily forecasts, weather alerts and reference data.

| Layer | Datasets |
|---|---|
| raw | MeteoTrentino ASMX / meteo.report / API Manager responses |
| staging | `stg_mt_meteo_stations`, `stg_mt_station_observations`, `stg_mt_forecasts_hourly`, `stg_mt_forecasts_daily`, `stg_mt_alerts`, `stg_mt_forecast_locations`, `stg_mt_sky_conditions` |
| silver | the same seven, curated |
| gold | `mt_stations`, `mt_observations_current`, `mt_forecast_hourly`, `mt_forecast_daily`, `mt_alerts_active`, `mt_heat_daily_obs` |

It also publishes the four **provider-neutral contract tables** consumed by the `weather`
facade — `mt_weather_forecast_hourly`, `mt_weather_forecast_daily`, `mt_weather_alerts`
and `mt_weather_current`, materialised under the aliases `weather__forecast_hourly`,
`weather__forecast_daily`, `weather__alerts` and `weather__current`.

## `owm` — OpenWeatherMap

One Call API 3.0 observations and forecasts. **Requires `OWM_API_KEY`.** Licence
constraints apply to redistribution.

| Layer | Datasets |
|---|---|
| raw | API responses |
| staging | `stg_forecast_stream` |
| silver | `weather_current`, `weather_minutely`, `weather_hourly`, `weather_daily`, `weather_alerts` |
| gold | `folgaria_weather_*` (per-location views) and an `rdf_weather_*` family publishing the same observations in a semantic, unpivoted shape |

The `rdf_weather_observations_unpivoted` model is the SOSA-shaped projection: one row per
observed property. See
[`.agents/knowledge/semantic-measurement-views.md`](https://github.com/celine-eu/celine-pipelines/blob/main/.agents/knowledge/semantic-measurement-views.md)
for why the unpivot is required rather than optional.

## `copernicus` — ERA5 and CAMS

Reanalysis from the Copernicus Climate Change Service and Atmosphere Monitoring Service.
**Requires `CDSAPI_KEY` and `CDSAPI_URL`.** Copernicus licence: free, attribution
required.

| Layer | Datasets |
|---|---|
| staging | `stg_era5_single_levels`, `stg_cams_global_reanalysis_eac4_monthly`, `stg_radiation_raw` |
| silver | `copernicus_era5_single_levels`, `copernicus_cams_global_reanalysis_eac4_monthly`, `copernicus_solar_radiation`, `copernicus_weather_features` |
| gold | `copernicus_era5_analysis` |

## `dwd` — ICON-D2 (paused)

> **Status: paused.** Wind functionality was replaced by the `om` wind flow, which reads
> the same ICON-D2 model through the Open-Meteo API. The DWD pipeline had an 8% success
> rate and a 12+ GB database footprint; `om-wind` achieves the same coverage at ~90 MB/day.
> The code is retained because it is the only path to raw GRIB2 fields.

| Layer | Datasets |
|---|---|
| staging | `stg_dwd_icon_d2` |
| silver | `dwd_icon_d2_silver`, `dwd_icon_d2_wind`, `dwd_icon_d2_solar_forecast_interval` |
| gold | `dwd_icon_d2_gold`, `dwd_icon_d2_gusts`, `dwd_icon_d2_solar_energy` |

## `weather` — multi-provider facade

A dbt-only pipeline with no ingestion of its own. It reads the four provider-neutral
`weather__*` contract tables from the gold schema and produces stable, deduplicated
consumer tables: for each entry in the `weather_locations` seed it keeps the nearest
provider location.

| Layer | Datasets |
|---|---|
| sources | `weather__forecast_hourly`, `weather__forecast_daily`, `weather__alerts`, `weather__current` (gold, any provider) |
| staging | `stg_weather_forecast_hourly`, `stg_weather_forecast_daily`, `stg_weather_alerts`, `stg_weather_current` |
| gold | `weather_forecast_hourly`, `weather_forecast_daily`, `weather_alerts_active`, `weather_current` |

Scheduled at `15 * * * *`, ten minutes after `mt`, so each hour's facade reflects that
hour's provider run.

**Downstream:** `digital-twin`, `celine-webapp`.

**Adding a provider** means writing that provider's gold model under the `weather__*`
alias. This pipeline needs no change.

---

# Geospatial

## `osm` — OpenStreetMap

Regional extracts curated into thematic layers for the areas the platform covers.
Licence: **ODbL-1.0**, attribution required.

| Layer | Datasets |
|---|---|
| staging | `openstreetmap_it_alpecimbra`, `openstreetmap_fi_lappeenranta` |
| silver | per-area thematic layers: `_base`, `_amusement`, `_ev_charging`, `_hospitality`, `_parking`, `_piste_and_lifts`, `_tourism_culture`, `_social_buildings` |

## `overture` — Overture Maps buildings

Building footprints for the Trentino region, the entry point of the PV chain.

| Layer | Datasets |
|---|---|
| raw | `raw.overture_buildings` |
| staging | `stg_overture_buildings` |
| silver | `pv_overture_buildings` — `building_id` is `md5(ST_AsText(geometry))`, stable across runs |

**Downstream:** `pv_estimation`, `pv_detection`, `trentino_rooftops`.

## `trentino_rooftops` — PV siting constraints

Regional open datasets describing where PV may and may not be installed.

| Layer | Datasets |
|---|---|
| staging | `stg_aree_non_idonee`, `stg_vincoli_diretti`, `stg_vincoli_indiretti` |
| silver | `pv_aree_non_idonee`, `pv_vincoli_diretti`, `pv_vincoli_indiretti` |
| gold | `pv_building_suitability` — per-building eligibility after constraint overlay |

Requires PostGIS.

---

# Photovoltaic analysis

## `pv_estimation` — rooftop PV ROI

Estimates the return on investment of a rooftop PV installation for each eligible
building, using the `celine-roi` library. Includes a Streamlit dashboard for policy makers
and REC managers at province and primary-substation level.

| Layer | Datasets |
|---|---|
| raw | `raw.pv_roi_estimates` — written by the Prefect flow, one row per building (NPV, IRR, payback, `tasso_autoconsumo`, regime) |
| staging | `stg_pv_roi_estimates` |
| silver | `pv_roi_estimates` |
| gold | `pv_rooftop_opportunities`, `pv_installation_ranking`, `pv_installation_plan`, `pv_installation_plan_summary`, `rec_building_cabina`, `rec_cabina_opportunities`, `rec_cabina_plan`, `rec_cabina_summary` |

Sources: `pv_overture_buildings` (overture, silver) and `gse_cabine_primarie` (rec_it,
gold). The ROI computation is a Python task, not dbt — `flows/roi_estimator.py`, run
incrementally with a `--full-refresh` escape hatch.

## `pv_detection` — existing PV from imagery

Detects PV already installed on rooftops from aerial orthophotos using a vision-language
model (Ollama, e.g. Qwen2.5-VL 7B). Building-driven: it queries `pv_overture_buildings`,
fetches the aerial tile covering each footprint, crops the rooftop and classifies it,
skipping crops where less than half the building is visible.

| Layer | Datasets |
|---|---|
| raw | `raw.pv_predictions` — `has_pv`, `confidence`, `model_name`, reasoning, centroid |
| staging | `stg_pv_predictions` |
| silver | `pv_detections` |
| gold | `pv_detected_installations` |

Complements `pv_estimation`: that one answers *should* a building have PV, this one
answers *does it already*.

> **Imagery licensing is not uniform.** Tile sources differ in what they permit for
> derived-data publication. Check the licence of the provider you configure before
> exposing any output.

---

# Grid

## `grid` — grid resilience

CIM-inspired wind and heat risk overlays for the distribution network, joining grid
topology with Open-Meteo forecasts. Requires PostGIS. dbt-only; no ingestion of its own.

| Flow | Cron | Purpose |
|---|---|---|
| `grid-resilience-daily` | `0 8 * * *` | Daily risk overlay, after the `om` wind and heat flows |
| `grid-nowcasting-15min` | `20/15 * * * *` | Nowcast refresh from 15-minute observations |

| Kind | Datasets |
|---|---|
| gold | `grid_network_topology`, `grid_substations`, `grid_shapes`, `grid_tiles`, `grid_tile_index`, `grid_wind_risks`, `grid_wind_risks_now`, `grid_heat_risks`, `grid_heat_risks_now`, `grid_risks`, `grid_risks_now`, `grid_risks_trendline`, `v_superset_grid` |

**Upstream (private):** two CIM-normalised silver tables produced by a DSO-specific
ingestion pipeline that is not part of this repository —
`silver_grid_ac_line_segment` and the substation table. The expected columns are declared
in `dbt/models/sources.yml`, which is the contract; `README.md` states the DDL so the
pipeline can be run against locally-created tables.

> A `column "strike_tree_*" does not exist` error means the local silver table predates an
> upstream change, **not** a bug in this pipeline. See
> [`.agents/knowledge/grid-strike-tree-columns-are-upstream.md`](https://github.com/celine-eu/celine-pipelines/blob/main/.agents/knowledge/grid-strike-tree-columns-are-upstream.md).

---

# REC and community

These four pipelines form a chain. `rec_metering` publishes the metering interface; `rec_it`
and `rec_flexibility` both read it; the two mirrors feed the raw tables those need.

```text
[private metering ingestion]        [rec-registry API]      [flexibility-api]
   silver.meters_data_normalized          |                        |
              |                     rec_registry            rec_flexibility_commitments
              v                            |                        |
        rec_metering                raw.rec_registry_mirror  raw.flexibility_commitments_mirror
   gold.meters_data_15m / _1h              |                        |
              |______________________ ____ | ______________________ |
                                     |                              |
                                     v                              v
                                  rec_it                      rec_flexibility
```

## `rec_metering` — metering interface

Promotes normalised 15-minute meter readings from silver into the gold tables every other
REC pipeline consumes. dbt-only.

| Kind | Datasets |
|---|---|
| source | `{{CELINE_SILVER_SCHEMA}}.meters_data_normalized` (private upstream) |
| gold | `meters_data_15m`, `meters_data_1h`, `meters_data_15m_missing_intervals`, `meters_measurements_15m` |

- `meters_data_15m` — deduplicated and summed per `(device_id, ts)`, incremental merge on
  `md5(device_id || ts)`. Energy in **kWh per 15-minute bucket**.
- `meters_data_1h` — hourly rollup of the above.
- `meters_data_15m_missing_intervals` — quality model. Generates the complete expected
  15-minute grid per device over the last 7 days and left-joins actuals, surfacing gaps.
- `meters_measurements_15m` — the SOSA-shaped unpivot of `meters_data_15m`. It shares its
  governance block with the parent **by YAML anchor**, deliberately: a semantic sibling
  that quietly lost its `rec_registry` row filter would serve every device's readings to
  every caller.

This is the smallest and most load-bearing pipeline in the repository. Its gold tables are
the contract for `rec_it` and `rec_flexibility`, so a change here is a change to both.

## `rec_it` — Italian CER settlement

Virtual self-consumption allocation per device under Italian GSE rules, plus the primary
substation reference layer.

| Flow | Cron | Purpose |
|---|---|---|
| `rec_it_flow` | `*/15 * * * *` | Settlement models |
| `rec_it_batches_flow` | `0 0 * * *` | Daily batch reprocessing |

| Kind | Datasets |
|---|---|
| sources | `meters_data_15m` (rec_metering, gold); `raw.rec_registry_mirror` (rec_registry); `raw.gse_cabine_primarie` (self-contained Meltano extractor) |
| silver | `silver_rec_registry`, `silver_gse_cabine_primarie` |
| gold | `gse_cabine_primarie`, `rec_virtual_consumption_15m`, `rec_virtual_consumption_hourly`, `rec_virtual_consumption_per_device_15m`, `rec_virtual_consumption_per_device_hourly`, `rec_measurements_15m`, `rec_measurements_per_device_15m` |

> **Per-substation netting is correct and must not be "simplified".** The community figure
> is `least()` per `(ts, rec_id, substation_id)` **and then** summed. Sharing cannot cross
> an unconnected *cabina primaria*; netting community-wide would invent energy that
> physically cannot flow. See
> [`.agents/knowledge/rec-virtual-consumption.md`](https://github.com/celine-eu/celine-pipelines/blob/main/.agents/knowledge/rec-virtual-consumption.md),
> which also records the known defect in the hourly model.

## `rec_flexibility` — flexibility and gamification

Opportunity windows, per-commitment settlement with proportional redistribution,
three-layer gamification scoring, anti-gaming flags and CO2 impact. Mixes dbt models with
Python tasks (baselines, streaks, auto-commit). `0 6 * * *`.

| Kind | Datasets |
|---|---|
| sources | `meters_data_15m` (rec_metering, gold); `meters_energy_forecast` and `total_meters_forecast` (meter forecasting, not in this repository); `raw.flexibility_commitments_mirror` |
| silver | `rec_meters_15m`, `silver_flexibility_commitments` |
| gold | `rec_flexibility_windows`, `rec_flexibility_windows_community`, `rec_device_baselines`, `rec_device_class`, `rec_device_streaks`, `rec_settlement_15m`, `rec_settlement_1h`, `rec_settlement_points`, `rec_commitment_settlement`, `rec_flexibility_bonus`, `rec_participant_points`, `rec_points_leaderboard`, `rec_gamification_summary`, `rec_anti_gaming_flags`, `rec_co2_savings`, `rec_co2_savings_community` |

Without the forecast tables the windows model produces no output — no surplus is detected,
and the pipeline succeeds with an empty result. That is the expected local behaviour, not
a failure.

> **The flexibility signal is netted across substations.** There is no join path from this
> app to `substation_id` at all; a deficit on one *cabina* cancels a surplus on another.
> See
> [`.agents/knowledge/flexibility-is-substation-blind.md`](https://github.com/celine-eu/celine-pipelines/blob/main/.agents/knowledge/flexibility-is-substation-blind.md).

## `rec_registry` — registry mirror

Full-replace mirror of the CELINE REC Registry API into `raw.rec_registry_mirror`, every
5 minutes. One row per user/community pair: grid areas, topology nodes, delivery points,
meter sensors. Python-only; no dbt, no Meltano. OIDC-authenticated.

## `rec_flexibility_commitments` — commitments mirror

90-day sliding mirror of the CELINE Flexibility API into
`raw.flexibility_commitments_mirror`, every 15 minutes, tracking status evolution
(accepted, settled, rejected, cancelled). Python-only. OIDC-authenticated.

---

## Pipelines with no `README.md`

`overture` and `trentino_rooftops` have no per-pipeline README yet; their entries above are
derived from the models and flow configuration. Adding one is the first thing to do when
next working in either.
