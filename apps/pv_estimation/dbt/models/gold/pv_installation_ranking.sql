{{ config(
    materialized='table',
    schema='gold'
) }}

with opportunities as (
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
        npv,
        irr,
        payback_simple,
        tasso_autoconsumo,
        geom_geojson
    from {{ ref('pv_rooftop_opportunities') }}
    where npv > 0
),

total as (
    select count(*) as total_buildings from opportunities
),

ranked as (
    select
        o.*,
        row_number() over (order by o.payback_simple asc, o.irr desc) as rank,
        t.total_buildings,
        round(
            100.0 * row_number() over (order by o.payback_simple asc, o.irr desc)
            / t.total_buildings, 2
        ) as cumulative_pct
    from opportunities o
    cross join total t
)

select * from ranked
