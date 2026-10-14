{{- config(materialized='view') -}}

select * from {{ source('om_extras', 'om_weather_current_extras') }}
