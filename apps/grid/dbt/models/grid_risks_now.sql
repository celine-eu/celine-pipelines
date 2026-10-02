{{ config(
    materialized='incremental',
    schema='gold',
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }}{% endif %}"
) }}

{#
    Collapsed nowcasting risk table — observation-based counterpart of grid_risks.

    Same structure as grid_risks (segment_id, date, risk_vector, risk_level,
    risk_color_hex, metrics) plus observed_at timestamp.
    Sources: grid_wind_risks_now + grid_heat_risks_now + grid_joint_heat_risks_now.
    WARNING and ALERT only — NORMAL excluded.

    Joint rows carry metrics -> 'asset_type' = 'joint' and the same segment_id
    grid_shapes mints for a joint, exactly as in the daily table.
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
        observed_at,
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
                date
            order by
                case risk_level when 'ALERT' then 1 else 2 end,
                gust_excess desc nulls last
        ) as rn
    from {{ ref('grid_wind_risks_now') }}
    where risk_level in ('ALERT', 'WARNING')

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
        observed_at,
        jsonb_build_object(
            'temp_max_c',            temp_max_c,
            'p90_threshold',         p90_threshold,
            'consecutive_heat_days', consecutive_heat_days,
            'altitude_band',         altitude_band,
            'thermal_tier',          thermal_tier,
            'thermal_modelled',      thermal_modelled,
            'thermal_margin_c',      thermal_margin_c,
            'thermal_theta_max_c',   thermal_theta_max_c,
            'thermal_insulation',    thermal_insulation,
            'heat_status',           heat_status,
            'soil_status',           soil_status,
            'soil7_mean_c',          soil7_mean_c,
            'soil7_p90_c',           soil7_p90_c,
            'soil_asof_date',        soil_asof_date,
            'air_heat_tier',         air_heat_tier,
            'escalated_by_thermal',  escalated_by_thermal
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
    from {{ ref('grid_heat_risks_now') }}
    where risk_level in ('ALERT', 'WARNING')

),

joint_ranked as (

    select
        md5(dso_id || '|' || 'joint' || '|' || joint_id::text) as segment_id,
        date,
        'heat'                  as risk_vector,
        risk_level,
        risk_color_hex,
        observed_at,
        jsonb_build_object(
            'temp_max_c',            temp_max_c,
            'p90_threshold',         p90_threshold,
            'consecutive_heat_days', consecutive_heat_days,
            'altitude_band',         altitude_band,
            'thermal_tier',          thermal_tier,
            'thermal_modelled',      true,
            'thermal_margin_c',      thermal_margin_c,
            'thermal_theta_max_c',   thermal_theta_max_c,
            'thermal_insulation',    thermal_insulation,
            'heat_status',           heat_status,
            'soil_status',           soil_status,
            'soil7_mean_c',          soil7_mean_c,
            'soil7_p90_c',           soil7_p90_c,
            'soil_asof_date',        soil_asof_date,
            'air_heat_tier',         air_heat_tier,
            'escalated_by_thermal',  escalated_by_thermal,
            'asset_type',            'joint',
            'joint_id',              joint_id,
            'technology',            technology,
            'anno_posa',             anno_posa,
            'is_asphalt',            is_asphalt,
            'm_r_critico',           m_r_critico
        )                       as metrics,
        row_number() over (
            partition by dso_id, joint_id, date
            order by
                case risk_level when 'ALERT' then 1 else 2 end,
                temp_max_c desc nulls last
        ) as rn
    from {{ ref('grid_joint_heat_risks_now') }}
    where risk_level in ('ALERT', 'WARNING')

)

select segment_id, date, risk_vector, risk_level, risk_color_hex, metrics, observed_at
from wind_ranked
where rn = 1

union all

select segment_id, date, risk_vector, risk_level, risk_color_hex, metrics, observed_at
from heat_ranked
where rn = 1

union all

select segment_id, date, risk_vector, risk_level, risk_color_hex, metrics, observed_at
from joint_ranked
where rn = 1
