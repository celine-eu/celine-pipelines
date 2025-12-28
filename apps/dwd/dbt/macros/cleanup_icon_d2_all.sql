{% macro cleanup_icon_d2_all() %}

  {{ log("=== ICON-D2 CLEANUP (expired forecasts only) ===", info=True) }}

  ---------------------------------------------------------------------------
  -- RAW
  ---------------------------------------------------------------------------
  {{ icon_d2_delete_expired_forecasts(
      relation=source('raw', 'dwd_icon_d2'),
      interval_end_column='interval_end_datetime',
      keep_days=1
  ) }}

  ---------------------------------------------------------------------------
  -- SILVER
  ---------------------------------------------------------------------------
  {{ icon_d2_delete_expired_forecasts(
      relation=ref('dwd_icon_d2_wind'),
      interval_end_column='interval_end_utc',
      keep_days=1
  ) }}

  {{ icon_d2_delete_expired_forecasts(
      relation=ref('dwd_icon_d2_solar_forecast_interval'),
      interval_end_column='interval_end_utc',
      keep_days=1
  ) }}

  ---------------------------------------------------------------------------
  -- GOLD
  ---------------------------------------------------------------------------
  {{ icon_d2_delete_expired_forecasts(
      relation=ref('dwd_icon_d2_gusts'),
      interval_end_column='date',
      keep_days=1
  ) }}

  {{ log("=== ICON-D2 CLEANUP DONE ===", info=True) }}

{% endmacro %}
