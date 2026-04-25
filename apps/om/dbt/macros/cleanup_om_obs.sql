{% macro cleanup_om_obs(
    schema_name='raw',
    table_name='om_observations',
    retention_days=1
) %}

{% set sql %}
  delete from {{ schema_name }}.{{ table_name }}
  where _sdc_extracted_at < now() - interval '{{ retention_days }} days'
  returning 1
{% endset %}

{% set result = run_query(sql) %}

{{ log("Deleted obs rows: " ~ (result.rows | length), info=True) }}

{% endmacro %}
