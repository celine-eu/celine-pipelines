-- Per date and vector the aggregated km must equal the km of the fragment rows
-- that received a weather match (date is not null) in the source models.
with agg as (
    select date, risk_vector, sum(km_total) as km
    from {{ ref('grid_risk_km') }}
    group by 1, 2
),
src as (
    select date, 'wind' as risk_vector, sum(length_m) / 1000.0 as km
    from {{ ref('grid_wind_risks') }}
    where date is not null
    group by 1
    union all
    select date, 'heat' as risk_vector, sum(length_m) / 1000.0 as km
    from {{ ref('grid_heat_risks') }}
    where date is not null
    group by 1
)
select agg.date, agg.risk_vector, agg.km as km_agg, src.km as km_src
from agg
full outer join src using (date, risk_vector)
where abs(coalesce(agg.km, 0) - coalesce(src.km, 0)) > 1e-3
