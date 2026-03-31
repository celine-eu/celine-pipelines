{{- config(materialized='view') -}}

{# Static reference table — full refresh, tiny #}
select *
from {{ source('raw', 'meteotrentino_sky_conditions') }}
