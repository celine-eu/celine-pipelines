{{ config(
    materialized='view',
    schema='gold',
    alias='weather_feature_of_interest'
  )
}}


SELECT DISTINCT
    location_id,
    lat,
    lon,
    CONCAT('weather:feature:', location_id) AS foi_iri
FROM {{ ref('weather_current') }}