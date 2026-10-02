{{ config(
    materialized='incremental',
    unique_key=['date', 'dso_id', 'joint_id'],
    on_schema_change='append_new_columns',
    schema='gold',
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }}{% endif %}"
) }}

{#
    Heat risk per MT cable joint from real-time observations (nowcasting): the
    observation-based twin of grid_joint_heat_risks.

    Same matrix on joint_tier, same restriction to joints the thermal model
    tiered, weather from grid_heat_status_now (nearest point within 15 km).
    NORMAL rows are dropped, like the cable nowcast.

    Joints carry no length and stay out of grid_risk_km, here as in the daily
    chain. Only joints that found a weather point are emitted (date is not
    null): an unmatched joint would produce a NULL-date row that the unique key
    (date, dso_id, joint_id) can never match.
#}

{% set joints = source('grid_silver', 'silver_grid_geo_thermal_joints') %}

with today_status as (

    select *
    from {{ ref('grid_heat_status_now') }}

),

with_dist as (

    select
        j.dso_id,
        j.joint_id,
        j.comune                                as municipality,

        j.joint_tier                            as thermal_tier,
        j.margin_min_c                          as thermal_margin_c,
        j.theta_max_c                           as thermal_theta_max_c,
        j.insulation_class                      as thermal_insulation,
        {#- technology is text in the source contract; the cast plus trim is a
            no-op on text and strips the JSON quoting on a jsonb fixture. -#}
        trim(both '"' from j.technology::text)  as technology,
        j.anno_posa,
        j.is_asphalt,
        j.m_r_critico,

        h.date,
        h.heat_status,
        h.soil_status,
        h.soil7_mean_c,
        h.soil7_p90_c,
        h.soil_asof_date,
        h.air_heat_tier,
        h.temp_max_c,
        h.p90_threshold,
        h.consecutive_heat_days,
        h.elevation_m,
        h.altitude_band,
        h.observed_at,
        ST_Distance(
            j.geom,
            ST_Transform(h.geoposition::geometry, 32632)
        ) as dist_m
    from {{ joints }} j
    left join today_status h
        on ST_DWithin(ST_Transform(h.geoposition::geometry, 32632), j.geom, 15000)
    where j.joint_tier is not null

),

ranked as (

    select
        *,
        row_number() over (
            partition by dso_id, joint_id
            order by dist_m asc nulls last
        ) as rn
    from with_dist

),

leveled as (

    select
        *,
        {{ grid_heat_matrix('heat_status', 'thermal_tier') }}    as risk_tier,
        {{ grid_heat_escalated('heat_status', 'thermal_tier') }} as escalated_by_thermal
    from ranked

)

select
    dso_id,
    joint_id,
    municipality,

    thermal_tier,
    thermal_margin_c,
    thermal_theta_max_c,
    thermal_insulation,
    technology,
    anno_posa,
    is_asphalt,
    m_r_critico,

    date,
    risk_tier             as risk_level,
    escalated_by_thermal,

    heat_status,
    soil_status,
    soil7_mean_c,
    soil7_p90_c,
    soil_asof_date,
    air_heat_tier,

    temp_max_c,
    p90_threshold,
    consecutive_heat_days,
    elevation_m,
    altitude_band,
    {{ grid_risk_color('risk_tier') }} as risk_color_hex,
    observed_at

from leveled
where rn = 1
  and date is not null
  and risk_tier is not null
  and risk_tier != 'NORMAL'
