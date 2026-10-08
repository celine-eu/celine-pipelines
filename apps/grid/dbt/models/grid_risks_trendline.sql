{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    schema='gold',
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }} WHERE date >= current_date{% endif %}"
) }}

{#
    Daily risk percentage indicator per risk vector and distribution system
    operator (dso_id): each operator's ratio counts only its own network.

    risk_ratio = (alert_count + warning_count) / total_segments
    where total_segments is conductor-type-aware:
      - wind denominator: overhead_bare + overhead_insulated segments only
      - heat denominator: underground_cable segments only

    Thresholds for day_risk_level:
      > 0.65 → ALERT  (majority of relevant network at risk)
      > 0.35 → WARNING
      else   → NORMAL

    Cable joints are excluded from the heat numerator: they are their own asset
    family with no length and no place in the segment denominator, so counting
    them here would push the ratio above what the tratte actually say.
#}

with totals as (

    select
        dso_id,
        count(*) filter (
            where conductor_type in ('overhead_bare', 'overhead_insulated')
        ) as wind_total,
        count(*) filter (
            where conductor_type = 'underground_cable'
        ) as heat_total
    from {{ ref('grid_shapes') }}
    where asset_type = 'ac_line_segment'
    group by dso_id

),

counts as (

    select
        dso_id,
        date,
        risk_vector,
        count(*) filter (where risk_level = 'ALERT')   as alert_count,
        count(*) filter (where risk_level = 'WARNING') as warning_count
    from {{ ref('grid_risks') }}
    where coalesce(metrics ->> 'asset_type', 'ac_line_segment') <> 'joint'
    {% if is_incremental() %}
      and date >= current_date
    {% endif %}
    group by dso_id, date, risk_vector

),

with_totals as (

    select
        c.dso_id,
        c.date,
        c.risk_vector,
        c.alert_count,
        c.warning_count,
        case c.risk_vector
            when 'wind' then t.wind_total
            else t.heat_total
        end as total_segments
    from counts c
    inner join totals t
        on t.dso_id = c.dso_id

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
    end                     as day_risk_level,
    dso_id

from with_totals
