{{
    config(
        materialized='view',
        tags=['rec_flexibility']
    )
}}

-- Analysis-ready 15-min meter table, thin view over the sanctioned
-- ds_dev_gold.meters_data_15m interface (rec_metering app, dataset-api exposed).
--
-- meters_data_15m stores per (device_id, ts):
--     consumption_kwh   = M1 consumption
--     production_kwh    = M1 grid export
--     self_consumed_kwh = Σ(M2, M2_2 production) − M1 export, UNCLIPPED
--
-- Because self_consumed is unclipped upstream, the M1/M2 merge of
-- gamification/data_loader.py::load_meters is reconstructed exactly:
--     pv_production_kwh = self_consumed_kwh + production_kwh   -- gross PV
--     self_consumed_kwh = clip(self_consumed_kwh, ≥0)
--     total_consumption_kwh = consumption_kwh + clipped self_consumed
--
-- Coverage note: meters_data_15m also carries M2-only slots (PV reading with no
-- M1 row, consumption = production = 0, self_consumed = the whole PV output).
-- They were kept here until 2026-09-07 "so self-consumption during M1 gaps
-- counts" — which booked 100% of PV as consumption whenever the grid meter was
-- silent: one prosumer was credited up to 117 kWh/day in August 2026 (81% of his
-- settlement points) on exactly those days. An M1 gap is missing data, not
-- consumption, so such slots are now DROPPED. Every downstream model (baselines,
-- device class, settlement points, leaderboard) inherits the rule from here.
-- A real M1 row reads as (0, 0) only when the grid exchange is exactly zero, in
-- which case self_consumed is 0 too and the row is kept.

-- Fleet scope: the registry's membership, every role (rec_registry's
-- rec_device_membership). A reading is kept only when its (device_id, community_id)
-- pair is a membership row, so a device listed under two communities contributes each
-- reading to the community the reading carries, once. This view is the one place the
-- fleet is decided; every downstream model inherits it, and every downstream model
-- carries the reading's community_id.
select
    m._id,
    m.device_id,
    m.community_id,
    m.ts,
    m.consumption_kwh,
    m.production_kwh,
    m.self_consumed_kwh + m.production_kwh                          as pv_production_kwh,
    greatest(m.self_consumed_kwh, 0.0)                              as self_consumed_kwh,
    m.consumption_kwh + greatest(m.self_consumed_kwh, 0.0)          as total_consumption_kwh
from {{ source('rec_metering_gold', 'meters_data_15m') }} m
join {{ source('rec_registry_gold', 'rec_device_membership') }} r
  on  r.device_id    = m.device_id
  and r.community_id = m.community_id
where true
  -- M1-gap guard: PV reading with no grid-meter row → not a measurement.
  and not (
        coalesce(m.consumption_kwh, 0.0) = 0.0
    and coalesce(m.production_kwh, 0.0) = 0.0
    and coalesce(m.self_consumed_kwh, 0.0) > 0.0
  )
