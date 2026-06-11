{% macro grid_risk_color(tier_col) %}
    {#
        Map a risk tier column to a hex color string.

        Expects: ALERT / WARNING / NORMAL

        Unknown tiers → '#808080' (gray)
    #}
    CASE {{ tier_col }}
        WHEN 'ALERT'   THEN '#D00000'
        WHEN 'WARNING' THEN '#F7D000'
        WHEN 'NORMAL'  THEN '#00A000'
        ELSE '#808080'
    END
{% endmacro %}
