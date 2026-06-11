{% macro grid_geojson_feature(
    geom_col,
    risk_col=None,
    color_col=None,
    extra_props_expr=None,
    geometry_type='line'
) %}
    {#
        Build a GeoJSON Feature text column for deck.gl visualization.

        geom_col        : geometry column (EPSG:32632), transformed to 4326 on output
        risk_col        : SQL expression for risk tier value (omit for static maps)
        color_col       : SQL expression for hex color (omit for static maps)
        extra_props_expr: raw SQL json_build_object(...) fragment for domain-specific properties
        geometry_type   : 'line'  → strokeColor / strokeWidth / fillColor
                          'point' → iconColor / iconSize
    #}
    json_build_object(
        'type',     'Feature',
        'geometry', ST_AsGeoJSON(ST_Transform({{ geom_col }}, 4326))::json,
        'properties', json_build_object(
            {%- if risk_col is not none %}
            'risk_level',    {{ risk_col }},
            'strokeColor',   {{ color_col }},
            'strokeWidth',   3,
            'strokeOpacity', 0.9,
            'fillColor',     {{ color_col }},
            {%- endif %}
            {%- if geometry_type == 'point' %}
            'iconColor', '#1E88E5',
            'iconSize',  8,
            {%- endif %}
            'extraProps', {{ extra_props_expr }}
        )
    )::text
{% endmacro %}
