{{- config(materialized='view') -}}

select * from {{ source('weather_contract', 'weather__current') }}
