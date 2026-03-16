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

## dwd — Deutscher Wetterdienst (ICON-D2)

Ingests gridded weather model output from DWD's ICON-D2 high-resolution regional model for Germany and surrounding areas.

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
