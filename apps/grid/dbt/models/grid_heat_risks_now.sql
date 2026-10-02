{{ config(
    materialized='incremental',
    unique_key=['date', 'dso_id', 'line_name', 'municipality', 'conductor_type', 'length_m'],
    on_schema_change='append_new_columns',
    schema='gold',
    pre_hook="{% if is_incremental() %}DELETE FROM {{ this }}{% endif %}"
) }}

{#
    Heat risk per underground cable segment from real-time observations
    (nowcasting): the observation-based twin of grid_heat_risks.

    Weather comes from grid_heat_status_now, which crosses the observed air
    heat tier of today with the latest complete soil reading; the asset side is
    the thermal tier of the cable. Same matrix (grid_heat_matrix macro), same
    'low' reading for arcs the thermal model does not cover.

    Source: silver_grid_ac_line_segment (conductor_type = underground_cable).
    Observations: grid_heat_status_now, nearest point within 15 km (the
    observation network is sparser than the forecast grid).
    Heat season: May-Sep only (the P90 thresholds are NULL outside it).
    NORMAL rows are dropped: the nowcast table only carries live risk.
#}

{% set seg = source('grid_silver', 'silver_grid_ac_line_segment') %}

with today_status as (

    select *
    from {{ ref('grid_heat_status_now') }}

),

with_dist as (

    select
        s.dso_id,
        s.line_name,
        s.conductor_type,
        s.parent_substation_name,
        s.operational_unit,
        s.feeder_id,
        s.municipality,
        s.length_m,

        coalesce(s.thermal_tier, 'low')     as thermal_tier,
        s.thermal_tier is not null          as thermal_modelled,
        s.thermal_margin_c,
        s.thermal_theta_max_c,
        s.thermal_insulation,

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
            s.geom,
            ST_Transform(h.geoposition::geometry, 32632)
        ) as dist_m
    from {{ seg }} s
    left join today_status h
        on ST_DWithin(ST_Transform(h.geoposition::geometry, 32632), s.geom, 15000)
    where s.conductor_type = 'underground_cable'

),

ranked as (

    select
        *,
        row_number() over (
            partition by line_name, municipality, conductor_type, length_m
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
    line_name,
    conductor_type,
    parent_substation_name,
    operational_unit,
    feeder_id,
    municipality,
    length_m,

    thermal_tier,
    thermal_modelled,
    thermal_margin_c,
    thermal_theta_max_c,
    thermal_insulation,

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
  and risk_tier is not null
  and risk_tier != 'NORMAL'
