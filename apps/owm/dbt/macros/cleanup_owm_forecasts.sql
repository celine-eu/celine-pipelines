{% macro cleanup_owm_forecasts(retention_months=3) %}

  {{ log("=== OWM CLEANUP (forecasts older than " ~ retention_months ~ " months) ===", info=True) }}

  ---------------------------------------------------------------------------
  -- INDEXES (idempotent — no-op if already exist)
  ---------------------------------------------------------------------------
  {% set index_sql %}
    CREATE INDEX IF NOT EXISTS idx_owm_weather_hourly_ts
      ON {{ ref('weather_hourly') }} (ts);

    CREATE INDEX IF NOT EXISTS idx_owm_weather_daily_ts
      ON {{ ref('weather_daily') }} (ts);

    CREATE INDEX IF NOT EXISTS idx_owm_weather_current_ts
      ON {{ ref('weather_current') }} (ts);

    CREATE INDEX IF NOT EXISTS idx_owm_weather_minutely_dt
      ON {{ ref('weather_minutely') }} (dt);

    CREATE INDEX IF NOT EXISTS idx_owm_weather_alerts_start_ts
      ON {{ ref('weather_alerts') }} (start_ts);
  {% endset %}
  {% do run_query(index_sql) %}

  ---------------------------------------------------------------------------
  -- SILVER
  ---------------------------------------------------------------------------
  {{ owm_delete_old_forecasts(
      relation=ref('weather_hourly'),
      ts_column='ts',
      retention_months=retention_months
  ) }}

  {{ owm_delete_old_forecasts(
      relation=ref('weather_daily'),
      ts_column='ts',
      retention_months=retention_months
  ) }}

  {{ owm_delete_old_forecasts(
      relation=ref('weather_current'),
      ts_column='ts',
      retention_months=retention_months
  ) }}

  {{ owm_delete_old_forecasts(
      relation=ref('weather_minutely'),
      ts_column='dt',
      retention_months=retention_months
  ) }}

  {{ owm_delete_old_forecasts(
      relation=ref('weather_alerts'),
      ts_column='start_ts',
      retention_months=retention_months
  ) }}

  {{ log("=== OWM CLEANUP DONE ===", info=True) }}

{% endmacro %}
