{{- config(materialized='view') -}}

{# Window filter: only rows ingested in the last 7 days — daily forecasts span further ahead #}
select *
from {{ source('raw', 'meteotrentino_forecasts_daily') }}
where _sdc_extracted_at >= now() - interval '7 days'
