{#
  MeteoTrentino raw table retention.

  All time-series and event tables in the raw schema are managed by the
  shared cold-storage service and must NOT be deleted here:

    raw.station_observations  — 15-min meteorological record (incremental, grows continuously)
    raw.alerts                — historical alert log (append per run)
    raw.forecasts_hourly      — historical sub-daily forecasts for NWP verification (append per run)
    raw.forecasts_daily       — historical daily forecasts for NWP verification (append per run)

  Reference tables (sky_conditions, meteo_stations, forecast_locations) are
  full-refresh/replace on every run and therefore self-managing.

  This macro is intentionally a no-op.
#}

{% macro cleanup_mt_observations() %}

{{ log("MT raw tables are managed by the cold-storage service — no cleanup performed.", info=True) }}

{% endmacro %}
