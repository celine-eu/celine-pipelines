{{ config(
    materialized='incremental',
    schema='gold',
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }} WHERE date >= current_date{% endif %}"
) }}

{#
    Length-weighted risk exposure per tratta, for tabular views and reports.

    Grain: date × risk_vector × dso_id × operational_unit × line_name ×
           municipality × conductor_type
    — the map "tratta" (grid_shapes ac_line_segment grain, same segment_id)
    split by operational unit, so that rollups per line and per operational
    unit are exact sums of fragment lengths (a few lines span several units).

    Aggregated from the fragment-grain intermediaries (grid_wind_risks /
    grid_heat_risks), not from grid_risks, because only the fragments carry
    length_m. The denominator km_total is the length of the fragments that
    received a weather match for the date — the conductor scope is implicit
    (wind → overhead conductors only, heat → underground cable only), which is
    the share "of the line subject to that risk".

    risk_index = 100 · (km_alert + 0.5 · km_warning) / km_total  ∈ [0, 100].
    km_tree_high / km_tree_mid are the static tree-strike exposure of the
    tratta (wind rows only; 0 for heat), km_thermal_high / km_thermal_mid the
    static thermal exposure (heat rows only; 0 for wind). On heat rows
    km_escalated is the thermal escalation: tier high under a warm soil status.

    Cable joints are deliberately absent: a joint has no length, so it cannot
    enter a km measure without distorting it. The DSO alert dispatcher reads
    this table, so it does not see joints, by design: joint risk is a map and
    detail-panel signal (grid_risks, grid_joint_heat_risks).

    Rollups (line, operational unit) are computed at query time by the DT
    value fetcher `risk_km`; the index is recomputed from the summed km.
#}

with wind as (

    select
        date,
        'wind'                      as risk_vector,
        dso_id,
        operational_unit,
        line_name,
        municipality,
        conductor_type,
        parent_substation_name,
        feeder_id,
        length_m,
        risk_level,
        gust_excess                 as metric,
        escalated_by_tree_strike    as escalated,
        strike_tree_tier,
        null::text                  as thermal_tier
    from {{ ref('grid_wind_risks') }}
    where date is not null
    {% if is_incremental() %}
      and date >= current_date
    {% endif %}

),

heat as (

    select
        date,
        'heat'                      as risk_vector,
        dso_id,
        operational_unit,
        line_name,
        municipality,
        conductor_type,
        parent_substation_name,
        feeder_id,
        length_m,
        risk_level,
        temp_max_c                  as metric,
        escalated_by_thermal        as escalated,
        null::text                  as strike_tree_tier,
        thermal_tier
    from {{ ref('grid_heat_risks') }}
    where date is not null
    {% if is_incremental() %}
      and date >= current_date
    {% endif %}

),

fragments as (

    select * from wind
    union all
    select * from heat

),

agg as (

    select
        date,
        risk_vector,
        dso_id,
        operational_unit,
        line_name,
        municipality,
        conductor_type,
        md5(
            dso_id || '|' || 'ac_line_segment' || '|' ||
            line_name || '|' || conductor_type || '|' || municipality
        )                                                               as segment_id,
        min(parent_substation_name)                                     as parent_substation_name,
        min(feeder_id)                                                  as feeder_id,
        count(*)                                                        as n_fragments,
        sum(length_m) / 1000.0                                          as km_total,
        coalesce(sum(length_m) filter (where risk_level = 'ALERT'),   0) / 1000.0 as km_alert,
        coalesce(sum(length_m) filter (where risk_level = 'WARNING'), 0) / 1000.0 as km_warning,
        coalesce(sum(length_m) filter (
            where risk_level is null or risk_level not in ('ALERT', 'WARNING')
        ), 0) / 1000.0                                                  as km_normal,
        coalesce(sum(length_m) filter (where escalated), 0) / 1000.0    as km_escalated,
        coalesce(sum(length_m) filter (where strike_tree_tier = 'high'), 0) / 1000.0 as km_tree_high,
        coalesce(sum(length_m) filter (where strike_tree_tier = 'mid'),  0) / 1000.0 as km_tree_mid,
        coalesce(sum(length_m) filter (where thermal_tier = 'high'), 0) / 1000.0 as km_thermal_high,
        coalesce(sum(length_m) filter (where thermal_tier = 'mid'),  0) / 1000.0 as km_thermal_mid,
        max(metric)                                                     as metric_max,
        case
            when bool_or(risk_level = 'ALERT')   then 'ALERT'
            when bool_or(risk_level = 'WARNING') then 'WARNING'
            else 'NORMAL'
        end                                                             as worst_level
    from fragments
    group by 1, 2, 3, 4, 5, 6, 7, 8

)

select
    *,
    round(
        (100.0 * (km_alert + 0.5 * km_warning) / nullif(km_total, 0))::numeric,
        2
    ) as risk_index
from agg
