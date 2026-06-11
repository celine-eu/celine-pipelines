{% macro grid_heat_p90_threshold(altitude_band_col, date_col) %}
    {#
        P90 daily Tmax thresholds (°C) by altitude band and month.
        Source: Crespi 1981-2010, Trentino.
        Returns NULL outside warm season (May–Sep) → no heat stress.
    #}
    CASE
        WHEN {{ altitude_band_col }} = 'low' THEN
            CASE extract(month from {{ date_col }})
                WHEN 5 THEN 22.0  WHEN 6 THEN 26.0
                WHEN 7 THEN 27.0  WHEN 8 THEN 27.0
                WHEN 9 THEN 23.0
            END
        WHEN {{ altitude_band_col }} = 'mid' THEN
            CASE extract(month from {{ date_col }})
                WHEN 5 THEN 18.0  WHEN 6 THEN 22.0
                WHEN 7 THEN 23.0  WHEN 8 THEN 23.0
                WHEN 9 THEN 19.0
            END
        WHEN {{ altitude_band_col }} = 'high' THEN
            CASE extract(month from {{ date_col }})
                WHEN 5 THEN 14.0  WHEN 6 THEN 18.0
                WHEN 7 THEN 19.0  WHEN 8 THEN 19.0
                WHEN 9 THEN 15.0
            END
    END
{% endmacro %}
