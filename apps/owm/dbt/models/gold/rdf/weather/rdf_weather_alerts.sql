{{ config(
    materialized='view',
    schema='gold',
    alias='weather_alerts'
  )
}}

SELECT
    CONCAT('weather:alert:', location_id, ':', start_ts) AS alert_iri,
    location_id,
    CONCAT('weather:feature:', location_id) AS foi_iri,
    sender_name,
    event,
    description,
    start_ts,
    end_ts
FROM {{ ref('weather_alerts') }}