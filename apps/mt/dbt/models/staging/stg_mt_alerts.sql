{{- config(materialized='view') -}}

{# Full refresh — source is a single-object endpoint, small table #}
select *
from {{ source('raw', 'meteotrentino_alerts') }}
