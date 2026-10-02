{% macro grid_heat_escalated(heat_status_col, thermal_tier_col) %}
    {#
        True when the thermal axis is what lifted the level: a tier-high asset
        under a warm soil status (ORANGE or RED). The same asset at tier low or
        mid would have scored one step lower on the matrix.

        Mirrors escalated_by_tree_strike on the wind vector, and feeds
        km_escalated in grid_risk_km. NULL-safe: never NULL, false by default.
    #}
    coalesce(
        {{ thermal_tier_col }} = 'high'
        and {{ heat_status_col }} in ('ORANGE', 'RED'),
        false
    )
{% endmacro %}
