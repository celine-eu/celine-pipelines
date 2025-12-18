{% macro cleanup_icon_d2_models() %}
  {{ log("Cleaning STAGING/SILVER/GOLD ICON-D2 tables ...", info=True) }}

  {% set max_batches = 200 %}
  {% set chunk_size  = 50000 %}

  {% set tables = [
      ref('stg_dwd_icon_d2'),
      ref('dwd_icon_d2_wind'),
      ref('dwd_icon_d2_gusts')
  ] %}

  {% for table in tables %}
      {{ log("Processing table: " ~ table ~ " ...", info=True) }}

      {# Determine timestamp column #}
      {% if table.identifier == 'dwd_icon_d2_gusts' %}
          {% set ts_column = 'date' %}
          {% set cutoff = "current_date" %}
      {% else %}
          {% set ts_column = 'datetime' %}
          {% set cutoff = "date_trunc('day', now())" %}
      {% endif %}

      {% for i in range(1, max_batches) %}

          {# Count rows to delete #}
          {% set count_sql %}
            SELECT count(*) AS n
            FROM {{ table }}
            WHERE {{ ts_column }} < {{ cutoff }};
          {% endset %}

          {% set count_result = run_query(count_sql) %}
          {% set rowcount = count_result.columns[0].values()[0] %}

          {{ log("  Batch " ~ i ~ " — rows pending deletion: " ~ rowcount, info=True) }}

          {% if rowcount == 0 %}
            {{ log("  ✓ Table clean: " ~ table, info=True) }}
            {% break %}
          {% endif %}

          {# Delete batch #}
          {% set del_sql %}
            WITH del AS (
                SELECT ctid
                FROM {{ table }}
                WHERE {{ ts_column }} < {{ cutoff }}
                LIMIT {{ chunk_size }}
            )
            DELETE FROM {{ table }} t
            USING del
            WHERE t.ctid = del.ctid;
          {% endset %}

          {% do run_query(del_sql) %}
      {% endfor %}
  {% endfor %}
{% endmacro %}
