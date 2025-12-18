{% macro cleanup_icon_d2_raw() %}
  {{ log("Batch-cleaning RAW: dwd_icon_d2 ...", info=True) }}

  {% set max_batches = 500 %}
  {% set chunk_size = 50000 %}

  {% for i in range(1, max_batches) %}

      {# 1. Count rows that match delete condition #}
      {% set count_sql %}
        SELECT count(*) AS n
        FROM {{ source('raw','dwd_icon_d2') }}
        WHERE datetime < date_trunc('day', now());
      {% endset %}

      {% set count_result = run_query(count_sql) %}
      {% set rowcount = count_result.columns[0].values()[0] %}

      {{ log("Batch " ~ i ~ ": " ~ rowcount ~ " rows pending deletion", info=True) }}

      {# stop condition #}
      {% if rowcount == 0 %}
          {{ log("No more rows to delete. Cleanup complete.", info=True) }}
          {% break %}
      {% endif %}

      {# 2. Delete next chunk #}
      {% set del_sql %}
        WITH del AS (
            SELECT ctid
            FROM {{ source('raw','dwd_icon_d2') }}
            WHERE datetime < date_trunc('day', now())
            LIMIT {{ chunk_size }}
        )
        DELETE FROM {{ source('raw','dwd_icon_d2') }} t
        USING del
        WHERE t.ctid = del.ctid;
      {% endset %}

      {% do run_query(del_sql) %}

  {% endfor %}
{% endmacro %}
