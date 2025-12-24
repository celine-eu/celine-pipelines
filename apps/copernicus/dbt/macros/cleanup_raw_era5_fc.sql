{% macro cleanup_raw_era5_fc(
    schema_name='raw',
    table_name='reanalysis_era5_single_levels'
) %}

{% set sql %}
  delete from raw.reanalysis_era5_single_levels
  where data_type = 'fc'
    and datetime < now()
  returning 1
{% endset %}

{% set result = run_query(sql) %}

{{ log("Deleted rows: " ~ (result.rows | length), info=True) }}

{% endmacro %}