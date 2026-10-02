{% macro grid_heat_matrix(heat_status_col, thermal_tier_col) %}
    {#
        The soil x thermal risk matrix, the single place it is written.

        Rows are the two-axis heat status of the location (grid_heat_status),
        columns the thermal tier of the asset (thermal margin of the cable or
        of the joint):

            heat_status | tier low / mid | tier high
            ------------+----------------+----------
            GREEN       | NORMAL         | NORMAL
            ORANGE      | NORMAL         | WARNING
            RED         | WARNING        | ALERT

        A NULL status (no weather point within reach) yields a NULL level, as
        the air-only model did: the asset is not assessed, it is not "safe".
        The tier is expected non-NULL on a risk row (unmodelled assets read
        'low'), so a NULL tier falls in the low/mid column.
    #}
    case
        when {{ heat_status_col }} is null then null
        when {{ heat_status_col }} = 'RED' then
            case when {{ thermal_tier_col }} = 'high' then 'ALERT' else 'WARNING' end
        when {{ heat_status_col }} = 'ORANGE' then
            case when {{ thermal_tier_col }} = 'high' then 'WARNING' else 'NORMAL' end
        else 'NORMAL'
    end
{% endmacro %}
