{% macro cleanup_weather_forecasts(retention_days=30) %}

  {% set cutoff = "now() - interval '" ~ (retention_days | int) ~ " days'" %}

  {{ log("=== WEATHER CLEANUP (forecasts older than " ~ retention_days ~ " days) ===", info=True) }}

  {# --- weather_forecast_hourly (incremental, grows with every run) --- #}
  {% set sql_hourly %}
    DELETE FROM {{ ref('weather_forecast_hourly') }}
    WHERE forecast_at < {{ cutoff }};
  {% endset %}
  {% do run_query(sql_hourly) %}
  {{ log("  weather_forecast_hourly pruned", info=True) }}

  {# --- weather_forecast_daily (incremental, grows with every run) --- #}
  {% set sql_daily %}
    DELETE FROM {{ ref('weather_forecast_daily') }}
    WHERE forecast_date < ({{ cutoff }})::date;
  {% endset %}
  {% do run_query(sql_daily) %}
  {{ log("  weather_forecast_daily pruned", info=True) }}

  {# --- weather_alerts_active (only active alerts, self-pruning via expiry filter) --- #}
  {# no cleanup needed — staging already filters to non-expired #}

  {# --- weather_current (full replace on each run, single row per location) --- #}
  {# no cleanup needed #}

  {{ log("=== WEATHER CLEANUP DONE ===", info=True) }}

{% endmacro %}
