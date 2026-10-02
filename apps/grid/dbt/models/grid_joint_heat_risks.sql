{{ config(
    materialized='incremental',
    unique_key=['date', 'dso_id', 'joint_id'],
    on_schema_change='append_new_columns',
    schema='gold'
) }}

{#
    Heat risk per MT cable joint (giunto).

    A joint is the weak point of a buried run: the same soil, a shorter thermal
    path and an insulation class of its own, so the physical model tiers it
    separately from the arcs around it. Same matrix as grid_heat_risks, on
    joint_tier instead of the cable tier, so the two asset families are read
    with one rule (grid_heat_matrix macro).

    Only joints the thermal model tiered are assessed: joint_tier NULL means no
    model, and an untiered joint would otherwise read 'low' and pretend to be
    safe. Those joints still appear on the map (grid_shapes, tier 'unmodelled'),
    they simply carry no risk row.

    A joint has no length, so it is deliberately absent from grid_risk_km: the
    km exposure and the DSO alert dispatcher stay a cable measure.

    Only joints that found a weather point are emitted (date is not null). An
    unmatched joint would otherwise produce a NULL-date row that the incremental
    unique key (date, dso_id, joint_id) can never match, so every run would add
    another copy of it.

    Source: silver_grid_geo_thermal_joints (geom EPSG:32632).
    Weather: grid_heat_status (nearest point within 5 km per joint per date).
    Date range: today + 2 days ahead.
#}

{% set joints = source('grid_silver', 'silver_grid_geo_thermal_joints') %}

with date_range as (

    select generate_series(
        {% if var('start_date', none) is not none %}
            '{{ var("start_date") }}'::date,
        {% else %}
            CURRENT_DATE,
        {% endif %}
        CURRENT_DATE + interval '2 days',
        interval '1 day'
    )::date as date

),

heat_latest as (

    select h.*
    from {{ ref('grid_heat_status') }} h
    join date_range d on h.date = d.date

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
        {#- technology is text in the source contract. Some fixtures and older
            exports still carry it as jsonb, where a plain cast would keep the
            JSON quoting; the trim is a no-op on text and yields RESINA rather
            than "RESINA" on jsonb. -#}
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
        h.forecast_model,
        ST_Distance(
            j.geom,
            ST_Transform(h.geoposition::geometry, 32632)
        ) as dist_m
    from {{ joints }} j
    left join heat_latest h
        on ST_DWithin(ST_Transform(h.geoposition::geometry, 32632), j.geom, 5000)
    where j.joint_tier is not null

),

ranked as (

    select
        *,
        row_number() over (
            partition by date, dso_id, joint_id
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
    forecast_model,
    {{ grid_risk_color('risk_tier') }} as risk_color_hex

from leveled
where rn = 1
  and date is not null
