{{- config(materialized='view') -}}

{# Reference table — full refresh, small #}
select *
from {{ source('raw', 'meteotrentino_forecast_locations') }}
