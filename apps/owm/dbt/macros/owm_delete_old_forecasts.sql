{% macro owm_delete_old_forecasts(
    relation,
    ts_column,
    retention_months=3,
    batch_size=10000
) %}

  {% if relation is none %}
    {{ exceptions.raise_compiler_error("relation is required") }}
  {% endif %}

  {% if ts_column is none %}
    {{ exceptions.raise_compiler_error("ts_column is required") }}
  {% endif %}

  {% set cutoff_expr = "now() - interval '" ~ (retention_months | int) ~ " months'" %}

  {{ log(
      "OWM cleanup → " ~ relation ~
      " | delete where " ~ ts_column ~ " < " ~ cutoff_expr,
      info=True
  ) }}

  {% set delete_sql %}
    DO $$
    DECLARE deleted int;
    BEGIN
      LOOP
        DELETE FROM {{ relation }}
        WHERE ctid IN (
          SELECT ctid FROM {{ relation }}
          WHERE {{ ts_column }} < {{ cutoff_expr }}
          LIMIT {{ batch_size }}
        );
        GET DIAGNOSTICS deleted = ROW_COUNT;
        EXIT WHEN deleted = 0;
      END LOOP;
    END $$;
  {% endset %}

  {% do run_query(delete_sql) %}

{% endmacro %}
