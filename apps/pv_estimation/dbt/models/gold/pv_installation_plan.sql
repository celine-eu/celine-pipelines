{{ config(
    materialized='table',
    schema='gold'
) }}

with ranked as (
    select * from {{ ref('pv_installation_ranking') }}
),

params as (
    select
        year,
        cumulative_adoption_pct,
        degradation_rate,
        lag(cumulative_adoption_pct, 1, 0) over (order by year) as prev_cumulative_pct
    from {{ ref('installation_plan_params') }}
),

assigned as (
    select
        r.building_id,
        r.geometry,
        r.building_class,
        r.user_type,
        r.footprint_area_m2,
        r.kwp,
        r.capex,
        r.annual_production_kwh,
        r.annual_consumption_kwh,
        r.npv,
        r.irr,
        r.payback_simple,
        r.tasso_autoconsumo,
        r.rank,
        r.cumulative_pct,
        r.geom_geojson,
        p.year as install_year,
        p.degradation_rate
    from ranked r
    inner join params p
        on r.cumulative_pct > p.prev_cumulative_pct
        and r.cumulative_pct <= p.cumulative_adoption_pct
)

select
    building_id,
    geometry,
    building_class,
    user_type,
    footprint_area_m2,
    kwp,
    capex,
    annual_production_kwh,
    annual_consumption_kwh,
    annual_production_kwh * tasso_autoconsumo as annual_self_consumed_kwh,
    annual_production_kwh * (1 - tasso_autoconsumo) as annual_grid_export_kwh,
    npv,
    irr,
    payback_simple,
    tasso_autoconsumo,
    rank,
    install_year,
    degradation_rate,
    geom_geojson
from assigned
