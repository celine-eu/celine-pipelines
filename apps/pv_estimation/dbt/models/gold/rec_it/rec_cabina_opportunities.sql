{{ config(
    materialized='table',
    schema='gold',
    indexes=[
        {'columns': ['cod_ac']}
    ]
) }}

with mapping as (
    select * from {{ ref('rec_building_cabina') }}
),

opportunities as (
    select * from {{ ref('pv_rooftop_opportunities') }}
)

select
    m.cod_ac,
    m.rag_soc,
    o.building_id,
    o.user_type,
    o.kwp,
    o.capex,
    o.annual_production_kwh,
    o.annual_consumption_kwh,
    o.npv,
    o.irr,
    o.payback_simple,
    o.tasso_autoconsumo,
    o.footprint_area_m2,
    o.geometry,
    o.geom_geojson
from opportunities o
join mapping m on m.building_id = o.building_id
