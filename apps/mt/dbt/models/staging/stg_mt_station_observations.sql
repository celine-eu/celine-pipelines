{{- config(materialized='view') -}}

{# Window filter: limit raw scan to the last 30 days by ingestion time #}
select *
from {{ source('raw', 'meteotrentino_station_observations') }}
where _sdc_extracted_at >= now() - interval '30 days'
