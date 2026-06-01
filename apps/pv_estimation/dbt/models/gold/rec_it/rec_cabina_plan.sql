{{ config(
    materialized='table',
    schema='gold',
    indexes=[
        {'columns': ['cod_ac']},
        {'columns': ['install_year']}
    ]
) }}

with mapping as (
    select * from {{ ref('rec_building_cabina') }}
),

plan as (
    select * from {{ ref('pv_installation_plan') }}
)

select
    m.cod_ac,
    m.rag_soc,
    p.install_year,

    count(*) as new_buildings,
    sum(count(*)) over (partition by m.cod_ac order by p.install_year) as cum_buildings,

    round(sum(p.kwp)::numeric, 1) as new_kwp,
    round(sum(sum(p.kwp)) over (partition by m.cod_ac order by p.install_year)::numeric, 1) as cum_kwp,

    round(sum(p.annual_production_kwh)::numeric / 1000, 1) as new_production_mwh,
    round(sum(p.annual_self_consumed_kwh)::numeric / 1000, 1) as new_self_consumed_mwh,
    round(sum(p.annual_grid_export_kwh)::numeric / 1000, 1) as new_grid_export_mwh,

    round(sum(p.capex)::numeric, 0) as new_investment_eur,
    round(sum(sum(p.capex)) over (partition by m.cod_ac order by p.install_year)::numeric, 0) as cum_investment_eur,

    round(sum(p.npv)::numeric, 0) as new_npv_eur,
    round((sum(p.kwp * p.irr) / nullif(sum(p.kwp), 0))::numeric, 4) as weighted_avg_irr,
    round(avg(p.payback_simple)::numeric, 1) as avg_payback_years

from plan p
join mapping m on m.building_id = p.building_id
group by m.cod_ac, m.rag_soc, p.install_year
