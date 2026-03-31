{{- config(materialized='view') -}}

{# Reference table — full refresh, small #}
select *
from {{ source('raw', 'meteotrentino_meteo_stations') }}
