{{ config(
    materialized='table',
    schema='gold'
) }}

with plan as (
    select * from {{ ref('pv_installation_plan') }}
),

total_stock as (
    select count(*) as total_eligible from {{ ref('pv_installation_ranking') }}
),

yearly as (
    select
        install_year,
        count(*) as new_buildings,
        round(sum(kwp)::numeric, 1) as new_kwp,
        round(sum(annual_production_kwh)::numeric / 1000, 1) as new_production_mwh,
        round(sum(annual_consumption_kwh)::numeric / 1000, 1) as new_consumption_mwh,
        round(sum(annual_production_kwh * tasso_autoconsumo)::numeric / 1000, 1) as new_self_consumed_mwh,
        round(sum(annual_production_kwh * (1 - tasso_autoconsumo))::numeric / 1000, 1) as new_grid_export_mwh,
        round(sum(capex)::numeric, 0) as new_investment_eur,
        round(sum(npv)::numeric, 0) as new_npv_eur,
        round((sum(kwp * irr) / nullif(sum(kwp), 0))::numeric, 4) as weighted_avg_irr,
        round(avg(payback_simple)::numeric, 1) as avg_payback_years,
        min(degradation_rate) as degradation_rate
    from plan
    group by install_year
),

cumulative as (
    select
        y.install_year,
        y.new_buildings,
        sum(y.new_buildings) over (order by y.install_year) as cum_buildings,
        y.new_kwp,
        round(sum(y.new_kwp) over (order by y.install_year)::numeric, 1) as cum_kwp,
        y.new_production_mwh,
        y.new_consumption_mwh,
        y.new_self_consumed_mwh,
        y.new_grid_export_mwh,
        y.new_investment_eur,
        round(sum(y.new_investment_eur) over (order by y.install_year)::numeric, 0) as cum_investment_eur,
        y.new_npv_eur,
        round(sum(y.new_npv_eur) over (order by y.install_year)::numeric, 0) as cum_npv_eur,
        y.weighted_avg_irr,
        y.avg_payback_years,
        y.degradation_rate,
        t.total_eligible
    from yearly y
    cross join total_stock t
)

select
    install_year,

    new_buildings,
    cum_buildings,
    round(100.0 * cum_buildings / total_eligible, 1) as coverage_pct,

    new_kwp,
    cum_kwp,
    round(cum_kwp / 1000, 2) as cum_mwp,

    new_production_mwh,
    new_consumption_mwh,
    new_self_consumed_mwh,
    new_grid_export_mwh,

    -- cumulative production accounting for degradation of earlier cohorts
    round((
        sum(new_production_mwh) over (order by install_year)
        - sum(
            new_production_mwh * degradation_rate * (install_year - 1)
        ) over (order by install_year)
    )::numeric, 1) as cum_annual_production_mwh,

    new_investment_eur,
    cum_investment_eur,
    round(cum_investment_eur / 1000000.0, 2) as cum_investment_meur,

    new_npv_eur,
    cum_npv_eur,

    weighted_avg_irr,
    avg_payback_years,

    total_eligible

from cumulative
order by install_year
