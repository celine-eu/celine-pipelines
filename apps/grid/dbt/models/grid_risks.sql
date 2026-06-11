{{ config(
    materialized='incremental',
    schema='gold',
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }} WHERE date >= current_date{% endif %}"
) }}

{#
    Collapsed risk table for frontend visualization.

    Merges wind and heat risk vectors into a single table — WARNING and ALERT
    only (NORMAL is filtered out at pipeline level since it is never shown on
    the map). Geometry is intentionally excluded; the frontend joins against
    grid_shapes by segment_id to obtain feature_geojson.

    segment_id hash includes municipality (matching grid_shapes).
    When a municipality has multiple sub-fragments with different risk levels,
    the worst risk (ALERT > WARNING) is kept.

    Vector-specific metrics are stored as JSONB so the frontend can render
    them as a key-value table in the detail panel without the backend needing
    separate endpoints per vector.
#}

with wind_ranked as (

    select
        md5(
            dso_id || '|' || 'ac_line_segment' || '|' ||
            line_name || '|' || conductor_type || '|' || municipality
        )                       as segment_id,
        date,
        'wind'                  as risk_vector,
        risk_level,
        risk_color_hex,
        jsonb_build_object(
            'gust_excess',    gust_excess,
            'wind_speed_max', wind_speed_max,
            'wind_gusts_max', wind_gusts_max
        )                       as metrics,
        row_number() over (
            partition by
                md5(dso_id || '|' || 'ac_line_segment' || '|' ||
                    line_name || '|' || conductor_type || '|' || municipality),
                date
            order by
                case risk_level when 'ALERT' then 1 else 2 end,
                gust_excess desc nulls last
        ) as rn
    from {{ ref('grid_wind_risks') }}
    where risk_level in ('ALERT', 'WARNING')
    {% if is_incremental() %}
      and date >= current_date
    {% endif %}

),

heat_ranked as (

    select
        md5(
            dso_id || '|' || 'ac_line_segment' || '|' ||
            line_name || '|' || conductor_type || '|' || municipality
        )                       as segment_id,
        date,
        'heat'                  as risk_vector,
        risk_level,
        risk_color_hex,
        jsonb_build_object(
            'temp_max_c',            temp_max_c,
            'p90_threshold',         p90_threshold,
            'consecutive_heat_days', consecutive_heat_days,
            'altitude_band',         altitude_band,
            'forecast_model',        forecast_model
        )                       as metrics,
        row_number() over (
            partition by
                md5(dso_id || '|' || 'ac_line_segment' || '|' ||
                    line_name || '|' || conductor_type || '|' || municipality),
                date
            order by
                case risk_level when 'ALERT' then 1 else 2 end,
                temp_max_c desc nulls last
        ) as rn
    from {{ ref('grid_heat_risks') }}
    where risk_level in ('ALERT', 'WARNING')
    {% if is_incremental() %}
      and date >= current_date
    {% endif %}

)

select segment_id, date, risk_vector, risk_level, risk_color_hex, metrics
from wind_ranked
where rn = 1

union all

select segment_id, date, risk_vector, risk_level, risk_color_hex, metrics
from heat_ranked
where rn = 1
