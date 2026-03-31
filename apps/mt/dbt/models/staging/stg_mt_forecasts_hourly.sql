{{- config(materialized='view') -}}

{# Window filter: only rows ingested in the last 3 days — forecasts are short-lived #}
select *
from {{ source('raw', 'meteotrentino_forecasts_hourly') }}
where _sdc_extracted_at >= now() - interval '3 days'
