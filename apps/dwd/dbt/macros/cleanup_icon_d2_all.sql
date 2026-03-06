{% macro cleanup_icon_d2_all() %}

  {{ log("=== ICON-D2 CLEANUP (expired forecasts only) ===", info=True) }}

  ---------------------------------------------------------------------------
  -- INDEXES (idempotent — no-op if already exist)
  ---------------------------------------------------------------------------
  {% set index_sql %}
    CREATE INDEX IF NOT EXISTS idx_dwd_icon_d2_interval_end
      ON {{ source('raw', 'dwd_icon_d2') }} (interval_end_datetime);

    CREATE INDEX IF NOT EXISTS idx_dwd_icon_d2_wind_interval_end
      ON {{ ref('dwd_icon_d2_wind') }} (interval_end_utc);

    CREATE INDEX IF NOT EXISTS idx_dwd_icon_d2_solar_interval_end
      ON {{ ref('dwd_icon_d2_solar_forecast_interval') }} (interval_end_utc);

    CREATE INDEX IF NOT EXISTS idx_dwd_icon_d2_gusts_date
      ON {{ ref('dwd_icon_d2_gusts') }} (date);

    CREATE INDEX IF NOT EXISTS idx_dwd_icon_d2_solar_energy_interval_end
      ON {{ ref('dwd_icon_d2_solar_energy') }} (interval_end_utc);
  {% endset %}
  {% do run_query(index_sql) %}

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

  {{ icon_d2_delete_expired_forecasts(
      relation=ref('dwd_icon_d2_solar_energy'),
      interval_end_column='interval_end_utc',
      keep_days=1
  ) }}

  {{ log("=== ICON-D2 CLEANUP DONE ===", info=True) }}

{% endmacro %}
