{{ config(
    materialized='incremental',
    on_schema_change='append_new_columns',
    schema='gold',
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }} WHERE date >= current_date{% endif %}"
) }}

{#
    Intra-day companion of grid_risks: WARNING/ALERT rows per segment_id and
    8-hour window (slot 0 = 00–08, 1 = 08–16, 2 = 16–24).

    Wind comes from grid_wind_risks_8h (window max gust excess, same tiers and
    tree-strike escalation). Heat has no intra-day source (the forecast is a
    daily max), so each daily heat row is repeated on the three windows of its
    day — the map shows the same heat level whichever window is selected.

    Same segment_id, colours and metrics layout as grid_risks, so the frontend
    reuses its shapes join and popup. Cable joints follow the heat rows: same
    daily row on the three windows of its day.
#}

with slots as (

    select s as slot from generate_series(0, 2) as s

),

wind_ranked as (

    select
        md5(
            dso_id || '|' || 'ac_line_segment' || '|' ||
            line_name || '|' || conductor_type || '|' || municipality
        )                       as segment_id,
        dso_id,
        date,
        window_start,
        slot,
        'wind'                  as risk_vector,
        risk_level,
        risk_color_hex,
        jsonb_build_object(
            'gust_excess',    gust_excess,
            'wind_speed_max', wind_speed_max,
            'wind_gusts_max', wind_gusts_max,
            'strike_tree_tier',         strike_tree_tier,
            'strike_tree_multiplier',   strike_tree_multiplier,
            'strike_density_per_km',    strike_density_per_km,
            'escalated_by_tree_strike', escalated_by_tree_strike
        )                       as metrics,
        row_number() over (
            partition by
                md5(dso_id || '|' || 'ac_line_segment' || '|' ||
                    line_name || '|' || conductor_type || '|' || municipality),
                window_start
            order by
                case risk_level when 'ALERT' then 1 else 2 end,
                gust_excess desc nulls last
        ) as rn
    from {{ ref('grid_wind_risks_8h') }}
    where risk_level in ('ALERT', 'WARNING')
      and line_name is not null and municipality is not null
    {% if is_incremental() %}
      and date >= current_date
    {% endif %}

),

heat_ranked as (

    select
        md5(
            h.dso_id || '|' || 'ac_line_segment' || '|' ||
            h.line_name || '|' || h.conductor_type || '|' || h.municipality
        )                       as segment_id,
        h.dso_id,
        h.date,
        (h.date::timestamp + (sl.slot * 8) * interval '1 hour') as window_start,
        sl.slot,
        'heat'                  as risk_vector,
        h.risk_level,
        h.risk_color_hex,
        jsonb_build_object(
            'temp_max_c',            h.temp_max_c,
            'p90_threshold',         h.p90_threshold,
            'consecutive_heat_days', h.consecutive_heat_days,
            'altitude_band',         h.altitude_band,
            'forecast_model',        h.forecast_model,
            'thermal_tier',          h.thermal_tier,
            'thermal_modelled',      h.thermal_modelled,
            'thermal_margin_c',      h.thermal_margin_c,
            'thermal_theta_max_c',   h.thermal_theta_max_c,
            'thermal_insulation',    h.thermal_insulation,
            'heat_status',           h.heat_status,
            'soil_status',           h.soil_status,
            'soil7_mean_c',          h.soil7_mean_c,
            'soil7_p90_c',           h.soil7_p90_c,
            'soil_asof_date',        h.soil_asof_date,
            'air_heat_tier',         h.air_heat_tier,
            'escalated_by_thermal',  h.escalated_by_thermal
        )                       as metrics,
        row_number() over (
            partition by
                md5(h.dso_id || '|' || 'ac_line_segment' || '|' ||
                    h.line_name || '|' || h.conductor_type || '|' || h.municipality),
                h.date, sl.slot
            order by
                case h.risk_level when 'ALERT' then 1 else 2 end,
                h.temp_max_c desc nulls last
        ) as rn
    from {{ ref('grid_heat_risks') }} h
    cross join slots sl
    where h.risk_level in ('ALERT', 'WARNING')
      and h.line_name is not null and h.municipality is not null
    {% if is_incremental() %}
      and h.date >= current_date
    {% endif %}

),

joint_ranked as (

    select
        md5(j.dso_id || '|' || 'joint' || '|' || j.joint_id::text) as segment_id,
        j.dso_id,
        j.date,
        (j.date::timestamp + (sl.slot * 8) * interval '1 hour') as window_start,
        sl.slot,
        'heat'                  as risk_vector,
        j.risk_level,
        j.risk_color_hex,
        jsonb_build_object(
            'temp_max_c',            j.temp_max_c,
            'p90_threshold',         j.p90_threshold,
            'consecutive_heat_days', j.consecutive_heat_days,
            'altitude_band',         j.altitude_band,
            'forecast_model',        j.forecast_model,
            'thermal_tier',          j.thermal_tier,
            'thermal_modelled',      true,
            'thermal_margin_c',      j.thermal_margin_c,
            'thermal_theta_max_c',   j.thermal_theta_max_c,
            'thermal_insulation',    j.thermal_insulation,
            'heat_status',           j.heat_status,
            'soil_status',           j.soil_status,
            'soil7_mean_c',          j.soil7_mean_c,
            'soil7_p90_c',           j.soil7_p90_c,
            'soil_asof_date',        j.soil_asof_date,
            'air_heat_tier',         j.air_heat_tier,
            'escalated_by_thermal',  j.escalated_by_thermal,
            'asset_type',            'joint',
            'joint_id',              j.joint_id,
            'technology',            j.technology,
            'anno_posa',             j.anno_posa,
            'is_asphalt',            j.is_asphalt,
            'm_r_critico',           j.m_r_critico
        )                       as metrics,
        row_number() over (
            partition by j.dso_id, j.joint_id, j.date, sl.slot
            order by
                case j.risk_level when 'ALERT' then 1 else 2 end,
                j.temp_max_c desc nulls last
        ) as rn
    from {{ ref('grid_joint_heat_risks') }} j
    cross join slots sl
    where j.risk_level in ('ALERT', 'WARNING')
    {% if is_incremental() %}
      and j.date >= current_date
    {% endif %}

)

select segment_id, date, window_start, slot, risk_vector, risk_level, risk_color_hex, metrics, dso_id
from wind_ranked
where rn = 1

union all

select segment_id, date, window_start, slot, risk_vector, risk_level, risk_color_hex, metrics, dso_id
from heat_ranked
where rn = 1

union all

select segment_id, date, window_start, slot, risk_vector, risk_level, risk_color_hex, metrics, dso_id
from joint_ranked
where rn = 1
