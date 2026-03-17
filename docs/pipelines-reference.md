# Pipelines Reference

## om — Open-Meteo

Ingests hourly weather data from the Open-Meteo API (free, no API key required). Supports three run modes.

| Layer | Dataset | Description |
|---|---|---|
| RAW | `raw.om_weather` | Verbatim API response |
| STAGING | `ds_dev_staging.stg_om_weather` | Type-cast and deduplicated |
| SILVER | `ds_dev_silver.om_weather_hourly` | 4 natural variables: `shortwave_radiation`, `cloud_cover`, `temperature_2m`, `precipitation` |
| GOLD | `ds_dev_gold.om_weather_features` | 29 ML features: temporal/Fourier encodings, rolling stats, thermal dynamics, interaction features |

**Run modes:**
- `forecast` — fetches the upcoming 7-day forecast (default daily mode)
- `historical` — backfills from a given `start_date`
- `both` — historical backfill followed by forecast

**Scheduling:** Daily at 06:00 UTC when running as a service.

---

## om-wind — Open-Meteo Wind (Trentino Grid)

Fetches hourly wind speed, gusts, and direction from the Open-Meteo API for a
4.4 km grid covering the Trentino region (~798 points). Uses the ICON-D2 model
(2.2 km native resolution) as backend. Replaces the DWD ICON-D2 wind pipeline.

This is a **separate Prefect flow** (`om-wind-flow`) within the same `om` app,
with its own config (`config_wind.yaml`) and dbt models under
`models/{staging,silver,gold}/wind/` tagged `wind`.

| Layer | Dataset | Description |
|---|---|---|
| RAW | `raw.om_weather_wind` | 798 grid points x 48h forecast, 3 wind variables (m/s) |
| STAGING | `ds_dev_staging.stg_om_wind` | Type-cast view: `wind_speed_ms`, `wind_gusts_ms`, `wind_direction_deg` |
| SILVER | `ds_dev_silver.om_wind_hourly` | Deduplicated by `(datetime, lat, lon)`, keeps latest extraction |
| GOLD | `ds_dev_gold.om_wind_gusts` | Daily max/avg wind per grid point, gust excess, PostGIS point, alert tiers |

**Grid:** 4.4 km resolution (0.04 deg), bounding box lat 45.70-46.50, lon 10.40-11.90
**API method:** POST (avoids GET URL length limits and per-location rate limiting)
**Gust thresholds:** WARNING >= 7.62 m/s, ALERT >= 12.46 m/s (gust excess)
**Scheduling:** Every 4 hours (`0 0,4,8,12,16,20 * * *`)
**No API key required.** Free tier, ~6 calls/day.

**Downstream consumers:**
- `demo3/pipelines` gold model `grid_wind_risks` (spatial join with power grid shapefiles)
- `demo3/pipelines` gold model `grid_wind_risks_linee` (spatial join with medium-voltage lines)

---

## dwd — Deutscher Wetterdienst (ICON-D2) [PAUSED]

Ingests gridded weather model output from DWD's ICON-D2 high-resolution regional model for Germany and surrounding areas.

> **Status: PAUSED.** Wind functionality replaced by `om-wind` pipeline (Open-Meteo API,
> same ICON-D2 model backend). DWD pipeline had 8% success rate and 12+ GB DB footprint.
> The `om-wind` pipeline achieves the same coverage with ~90 MB/day and 99.9% reliability.

| Layer | Dataset | Description |
|---|---|---|
| RAW | `raw.dwd_icon_d2` | GRIB2/NetCDF model output, verbatim |
| STAGING | `staging.stg_dwd_icon_d2` | Grid extraction and normalization |
| SILVER | `silver.dwd_icon_d2_hourly` | Point-extraction for configured locations |

**Data sources:** DWD Open Data portal (opendata.dwd.de)
**License:** DWD Open Data License (free, attribution required)

---

## owm — OpenWeatherMap

Ingests current weather and forecasts from the OpenWeatherMap API.

| Layer | Dataset | Description |
|---|---|---|
| RAW | `raw.owm_weather` | API JSON responses |
| STAGING | `staging.stg_owm_weather` | Normalized weather observations |
| SILVER | `silver.owm_weather_hourly` | Hourly aggregated weather per location |

**Requires:** `OWM_API_KEY` environment variable.

---

## copernicus — ERA5 and CAMS

Ingests reanalysis data from the Copernicus Climate Change Service (ERA5) and Atmosphere Monitoring Service (CAMS).

| Dataset | Description |
|---|---|
| ERA5 | Historical hourly reanalysis: 2m temperature, radiation, wind, precipitation |
| CAMS | Atmospheric composition: ozone, NO2, PM2.5, aerosol optical depth |

**Requires:** `CDSAPI_KEY` (Copernicus CDS API key) and `CDSAPI_URL`.
**License:** Copernicus License (free, attribution required)

---

## osm — OpenStreetMap

Ingests and curates geospatial data from OpenStreetMap for energy community geographic context.

| Layer | Dataset | Description |
|---|---|---|
| RAW | `raw.osm_features` | Raw OSM element data |
| STAGING | `staging.stg_osm_features` | Normalized geometry and tag extraction |
| SILVER | `silver.osm_energy_features` | Energy-relevant features: buildings, roads, land use |

**Data source:** OSM Overpass API or PBF extract
**License:** ODbL-1.0 (Open Database License, attribution required)
