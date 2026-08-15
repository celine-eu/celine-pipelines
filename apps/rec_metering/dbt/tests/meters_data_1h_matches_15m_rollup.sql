-- The hourly table must equal the sum of its four 15-minute slots.
--
-- Both models are incremental merges over the same base, with independent
-- lookback windows. Nothing in dbt reconciles them: a merge that lands in one
-- table and not the other, or a change to one model's aggregation that is not
-- mirrored in the other, produces two tables that disagree about the same hour
-- and no error anywhere. Consumers pick whichever resolution suits them, so the
-- disagreement surfaces as two dashboards showing different totals.
--
-- Restricted to hours where the 15-minute table holds all four slots. A partial
-- hour is expected during ingestion and after cold-storage eviction, and would
-- otherwise report every boundary hour as a defect.
--
-- Tolerance rather than equality because both columns are double precision and
-- the two sides sum in different orders.

with fifteen as (
    select
        device_id,
        date_trunc('hour', ts) as ts,
        sum(consumption_kwh)   as consumption_kwh,
        sum(production_kwh)    as production_kwh,
        count(*)               as slots
    from {{ ref('meters_data_15m') }}
    group by device_id, date_trunc('hour', ts)
),

hourly as (
    select
        device_id,
        ts,
        consumption_kwh,
        production_kwh
    from {{ ref('meters_data_1h') }}
)

select
    h.device_id,
    h.ts,
    f.consumption_kwh as expected_consumption_kwh,
    h.consumption_kwh as actual_consumption_kwh,
    f.production_kwh  as expected_production_kwh,
    h.production_kwh  as actual_production_kwh
from hourly h
join fifteen f
  on f.device_id = h.device_id
 and f.ts        = h.ts
where f.slots = 4
  and (
        abs(coalesce(h.consumption_kwh, 0) - coalesce(f.consumption_kwh, 0)) > 1e-6
     or abs(coalesce(h.production_kwh,  0) - coalesce(f.production_kwh,  0)) > 1e-6
      )
