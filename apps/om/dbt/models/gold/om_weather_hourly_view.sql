{{ config(
    materialized = 'view',
    schema       = 'gold'
) }}

{#
    Gold view exposing silver hourly weather data for Superset dashboards.
    No transformation — direct pass-through from silver layer.
#}

select * from {{ ref('om_weather_hourly') }}
