{{ config(
    materialized='view',
    schema='gold',
    alias='weather_observed_properties'
  )
}}

SELECT * FROM (VALUES
    ('temperature',  'Air Temperature'),
    ('humidity',     'Relative Humidity'),
    ('pressure',     'Air Pressure'),
    ('uvi',          'UV Index'),
    ('clouds',       'Cloud Cover'),
    ('wind_deg',     'Wind Direction'),
    ('precipitation','Precipitation'),
    ('pop',          'Probability of Precipitation')
) AS t(code, label)