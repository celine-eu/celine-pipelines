{% macro cleanup_om_wind(
    schema_name='raw',
    table_name='om_weather_wind',
    retention_days=2
) %}

{% set sql %}
  delete from {{ schema_name }}.{{ table_name }}
  where _sdc_extracted_at < now() - interval '{{ retention_days }} days'
  returning 1
{% endset %}

{% set result = run_query(sql) %}

{{ log("Deleted wind rows: " ~ (result.rows | length), info=True) }}

{% endmacro %}
