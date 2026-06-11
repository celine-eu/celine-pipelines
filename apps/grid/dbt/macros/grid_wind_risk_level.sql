{% macro grid_wind_risk_level(wind_gusts_col, wind_speed_col) %}
    {#
        Classify wind gust excess into risk level.
        Thresholds from DWD ICON-D2 gust calibration.
    #}
    CASE
        WHEN ({{ wind_gusts_col }} - {{ wind_speed_col }}) >= 12.46 THEN 'ALERT'
        WHEN ({{ wind_gusts_col }} - {{ wind_speed_col }}) >= 7.62  THEN 'WARNING'
        ELSE 'NORMAL'
    END
{% endmacro %}
