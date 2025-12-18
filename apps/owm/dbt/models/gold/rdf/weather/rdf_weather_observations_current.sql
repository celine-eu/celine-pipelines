{{ config(
    materialized='view',
    schema='gold',
    alias='weather_observations_current'
  )
}}
SELECT
    CONCAT('weather:obs:current:', location_id, ':', ts) AS observation_iri,
    ts AS result_time,
    location_id,
    CONCAT('weather:feature:', location_id) AS foi_iri,
    'current' AS timescale,
    temp, humidity, pressure, uvi, clouds, wind_deg,
    weather_main, weather_description
FROM {{ ref('weather_current') }}
