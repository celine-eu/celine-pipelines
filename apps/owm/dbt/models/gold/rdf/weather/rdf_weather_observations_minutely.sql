{{ config(
    materialized='view',
    schema='gold',
    alias='weather_observations_minutely'
  )
}}
SELECT
    CONCAT('weather:obs:minutely:', location_id, ':', dt) AS observation_iri,
    dt AS result_time,
    location_id,
    CONCAT('weather:feature:', location_id) AS foi_iri,
    'minutely' AS timescale,
    precipitation
FROM {{ ref('weather_minutely') }}