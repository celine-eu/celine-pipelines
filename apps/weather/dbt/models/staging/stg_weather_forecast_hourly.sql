{{- config(materialized='view') -}}

select * from {{ source('weather_contract', 'weather__forecast_hourly') }}
