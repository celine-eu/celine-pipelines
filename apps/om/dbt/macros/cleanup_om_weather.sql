{% macro cleanup_om_weather(
    schema_name='raw',
    table_name='weather_hourly',
    retention_days=30
) %}

{% set sql %}
  delete from {{ schema_name }}.{{ table_name }}
  where _sdc_extracted_at < now() - interval '{{ retention_days }} days'
  returning 1
{% endset %}

{% set result = run_query(sql) %}

{{ log("Deleted rows: " ~ (result.rows | length), info=True) }}

{% endmacro %}
