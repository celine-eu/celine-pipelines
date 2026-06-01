{{ config(
    materialized='table',
    schema='gold',
    indexes=[
        {'columns': ['cod_ac']}
    ]
) }}

select
    cod_ac,
    rag_soc,

    count(*) as buildings,

    round(sum(kwp)::numeric, 1) as total_kwp,
    round(sum(kwp)::numeric / 1000, 2) as total_mwp,

    round(sum(annual_production_kwh)::numeric / 1000, 1) as total_production_mwh,
    round(sum(annual_consumption_kwh)::numeric / 1000, 1) as total_consumption_mwh,
    round(sum(annual_production_kwh * tasso_autoconsumo)::numeric / 1000, 1) as total_self_consumed_mwh,
    round(sum(annual_production_kwh * (1 - tasso_autoconsumo))::numeric / 1000, 1) as total_grid_export_mwh,

    round(sum(capex)::numeric, 0) as total_investment_eur,
    round(sum(capex)::numeric / 1000000, 2) as total_investment_meur,

    round(sum(npv)::numeric, 0) as total_npv_eur,
    round((sum(kwp * irr) / nullif(sum(kwp), 0))::numeric, 4) as weighted_avg_irr,
    round(avg(payback_simple)::numeric, 1) as avg_payback_years,
    round(avg(tasso_autoconsumo)::numeric, 4) as avg_autoconsumo,

    count(*) filter (where user_type = 'residential') as residential_buildings,
    count(*) filter (where user_type = 'commercial') as commercial_buildings,
    count(*) filter (where user_type = 'industrial') as industrial_buildings

from {{ ref('rec_cabina_opportunities') }}
group by cod_ac, rag_soc
