{{ config(
    materialized='incremental',
    schema='gold',
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }} WHERE date >= current_date{% endif %}"
) }}

{#
    Daily risk percentage indicator per risk vector.

    risk_ratio = (alert_count + warning_count) / total_segments
    where total_segments is conductor-type-aware:
      - wind denominator: overhead_bare + overhead_insulated segments only
      - heat denominator: underground_cable segments only

    Thresholds for day_risk_level:
      > 0.65 → ALERT  (majority of relevant network at risk)
      > 0.35 → WARNING
      else   → NORMAL
#}

with totals as (

    select
        count(*) filter (
            where conductor_type in ('overhead_bare', 'overhead_insulated')
        ) as wind_total,
        count(*) filter (
            where conductor_type = 'underground_cable'
        ) as heat_total
    from {{ ref('grid_shapes') }}
    where asset_type = 'ac_line_segment'

),

counts as (

    select
        date,
        risk_vector,
        count(*) filter (where risk_level = 'ALERT')   as alert_count,
        count(*) filter (where risk_level = 'WARNING') as warning_count
    from {{ ref('grid_risks') }}
    {% if is_incremental() %}
    where date >= current_date
    {% endif %}
    group by date, risk_vector

),

with_totals as (

    select
        c.date,
        c.risk_vector,
        c.alert_count,
        c.warning_count,
        case c.risk_vector
            when 'wind' then t.wind_total
            else t.heat_total
        end as total_segments
    from counts c
    cross join totals t

)

select
    date,
    risk_vector,
    alert_count,
    warning_count,
    total_segments,
    round(
        (alert_count + warning_count)::numeric / nullif(total_segments, 0),
        4
    )                       as risk_ratio,
    case
        when (alert_count + warning_count)::float
             / nullif(total_segments, 0) > 0.65 then 'ALERT'
        when (alert_count + warning_count)::float
             / nullif(total_segments, 0) > 0.35 then 'WARNING'
        else 'NORMAL'
    end                     as day_risk_level

from with_totals
