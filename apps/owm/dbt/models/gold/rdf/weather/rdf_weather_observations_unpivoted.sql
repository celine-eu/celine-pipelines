{{ config(
    materialized='view',
    schema='gold',
    alias='weather_observations_unpivoted'
) }}

WITH current AS (

    SELECT
        md5(concat('current|', location_id, '|', result_time::text, '|Temperature')) AS observation_id,
        location_id,
        result_time,
        'Temperature' AS property,
        temp::text AS value,
        'unit:DegreeCelsius' AS unit
    FROM {{ ref("rdf_weather_observations_current") }}

    UNION ALL
    SELECT md5(concat('current|', location_id, '|', result_time::text, '|Humidity')),
           location_id, result_time, 'Humidity', humidity::text, 'unit:Percent'
    FROM {{ ref("rdf_weather_observations_current") }}

    UNION ALL
    SELECT md5(concat('current|', location_id, '|', result_time::text, '|Pressure')),
           location_id, result_time, 'Pressure', pressure::text, 'unit:HectoPASCal'
    FROM {{ ref("rdf_weather_observations_current") }}

    UNION ALL
    SELECT md5(concat('current|', location_id, '|', result_time::text, '|UVI')),
           location_id, result_time, 'UVI', uvi::text, NULL
    FROM {{ ref("rdf_weather_observations_current") }}

    UNION ALL
    SELECT md5(concat('current|', location_id, '|', result_time::text, '|CloudCover')),
           location_id, result_time, 'CloudCover', clouds::text, 'unit:Percent'
    FROM {{ ref("rdf_weather_observations_current") }}

    UNION ALL
    SELECT md5(concat('current|', location_id, '|', result_time::text, '|WindDirection')),
           location_id, result_time, 'WindDirection', wind_deg::text, 'unit:Degree'
    FROM {{ ref("rdf_weather_observations_current") }}

    UNION ALL
    SELECT md5(concat('current|', location_id, '|', result_time::text, '|WeatherCondition')),
           location_id, result_time, 'WeatherCondition', weather_description, NULL
    FROM {{ ref("rdf_weather_observations_current") }}

),

hourly AS (

    SELECT
        md5(concat('hourly|', location_id, '|', result_time::text, '|Temperature')) AS observation_id,
        location_id, result_time, 'Temperature', temp::text, 'unit:DegreeCelsius'
    FROM {{ ref("rdf_weather_observations_hourly") }}

    UNION ALL
    SELECT md5(concat('hourly|', location_id, '|', result_time::text, '|Humidity')),
           location_id, result_time, 'Humidity', humidity::text, 'unit:Percent'
    FROM {{ ref("rdf_weather_observations_hourly") }}

    UNION ALL
    SELECT md5(concat('hourly|', location_id, '|', result_time::text, '|Pressure')),
           location_id, result_time, 'Pressure', pressure::text, 'unit:HectoPASCal'
    FROM {{ ref("rdf_weather_observations_hourly") }}

    UNION ALL
    SELECT md5(concat('hourly|', location_id, '|', result_time::text, '|UVI')),
           location_id, result_time, 'UVI', uvi::text, NULL
    FROM {{ ref("rdf_weather_observations_hourly") }}

    UNION ALL
    SELECT md5(concat('hourly|', location_id, '|', result_time::text, '|CloudCover')),
           location_id, result_time, 'CloudCover', clouds::text, 'unit:Percent'
    FROM {{ ref("rdf_weather_observations_hourly") }}

    UNION ALL
    SELECT md5(concat('hourly|', location_id, '|', result_time::text, '|WindDirection')),
           location_id, result_time, 'WindDirection', wind_deg::text, 'unit:Degree'
    FROM {{ ref("rdf_weather_observations_hourly") }}

    UNION ALL
    SELECT md5(concat('hourly|', location_id, '|', result_time::text, '|WeatherCondition')),
           location_id, result_time, 'WeatherCondition', weather_description, NULL
    FROM {{ ref("rdf_weather_observations_hourly") }}

),

daily AS (

    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|TemperatureDay')),
           location_id, result_time, 'TemperatureDay', temp_day::text, 'unit:DegreeCelsius'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|TemperatureMin')),
           location_id, result_time, 'TemperatureMin', temp_min::text, 'unit:DegreeCelsius'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|TemperatureMax')),
           location_id, result_time, 'TemperatureMax', temp_max::text, 'unit:DegreeCelsius'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|TemperatureNight')),
           location_id, result_time, 'TemperatureNight', temp_night::text, 'unit:DegreeCelsius'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|TemperatureEvening')),
           location_id, result_time, 'TemperatureEvening', temp_eve::text, 'unit:DegreeCelsius'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|TemperatureMorning')),
           location_id, result_time, 'TemperatureMorning', temp_morn::text, 'unit:DegreeCelsius'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|FeelsLikeDay')),
           location_id, result_time, 'FeelsLikeDay', feels_like_day::text, 'unit:DegreeCelsius'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|FeelsLikeNight')),
           location_id, result_time, 'FeelsLikeNight', feels_like_night::text, 'unit:DegreeCelsius'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|FeelsLikeEvening')),
           location_id, result_time, 'FeelsLikeEvening', feels_like_eve::text, 'unit:DegreeCelsius'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|FeelsLikeMorning')),
           location_id, result_time, 'FeelsLikeMorning', feels_like_morn::text, 'unit:DegreeCelsius'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|Humidity')),
           location_id, result_time, 'Humidity', humidity::text, 'unit:Percent'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|Pressure')),
           location_id, result_time, 'Pressure', pressure::text, 'unit:HectoPASCal'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|UVI')),
           location_id, result_time, 'UVI', uvi::text, NULL
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|Rainfall')),
           location_id, result_time, 'Rainfall', rain::text, 'unit:Millimeter'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|CloudCover')),
           location_id, result_time, 'CloudCover', clouds::text, 'unit:Percent'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|WindDirection')),
           location_id, result_time, 'WindDirection', wind_deg::text, 'unit:Degree'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|WindGust')),
           location_id, result_time, 'WindGust', wind_gust::text, 'unit:MeterPerSecond'
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|WeatherCondition')),
           location_id, result_time, 'WeatherCondition', weather_description, NULL
    FROM {{ ref("rdf_weather_observations_daily") }}

    UNION ALL
    SELECT md5(concat('daily|', location_id, '|', result_time::text, '|Summary')),
           location_id, result_time, 'Summary', summary, NULL
    FROM {{ ref("rdf_weather_observations_daily") }}
),

minutely AS (

    SELECT
        md5(concat('minutely|', location_id, '|', result_time::text, '|Precipitation')) AS observation_id,
        location_id,
        result_time,
        'Precipitation' AS property,
        precipitation::text AS value,
        'unit:Millimeter' AS unit
    FROM {{ ref("rdf_weather_observations_minutely") }}
)

SELECT * FROM current
UNION ALL
SELECT * FROM hourly
UNION ALL
SELECT * FROM daily
UNION ALL
SELECT * FROM minutely
