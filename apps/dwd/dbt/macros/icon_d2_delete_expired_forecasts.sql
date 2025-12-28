{% macro icon_d2_delete_expired_forecasts(
    relation,
    interval_end_column,
    keep_days=1
) %}

  {% if relation is none %}
    {{ exceptions.raise_compiler_error("relation is required") }}
  {% endif %}

  {% if interval_end_column is none %}
    {{ exceptions.raise_compiler_error("interval_end_column is required") }}
  {% endif %}

  {% set cutoff_expr = "date_trunc('day', now()) - interval '" ~ (keep_days | int) ~ " day'" %}

  {{ log(
      "ICON-D2 cleanup → " ~ relation ~
      " | delete where " ~ interval_end_column ~ " < " ~ cutoff_expr,
      info=True
  ) }}

  {% set delete_sql %}
    DELETE FROM {{ relation }}
    WHERE {{ interval_end_column }} < {{ cutoff_expr }};
  {% endset %}

  {% do run_query(delete_sql) %}

{% endmacro %}