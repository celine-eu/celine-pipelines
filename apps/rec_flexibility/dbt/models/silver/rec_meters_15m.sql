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
-- M1 row, consumption = production = 0). The previous silver-derived version
-- dropped them; they are kept here so self-consumption during M1 gaps counts.

-- v2 fleet scope: restrict to the active 11-device fleet (seed kept in sync with
-- flexibility_config.yaml fleet.active_devices). All downstream models inherit it.
with active as (
    select device_id from {{ ref('rec_active_devices') }}
)
select
    m._id,
    m.device_id,
    m.ts,
    m.consumption_kwh,
    m.production_kwh,
    m.self_consumed_kwh + m.production_kwh                          as pv_production_kwh,
    greatest(m.self_consumed_kwh, 0.0)                              as self_consumed_kwh,
    m.consumption_kwh + greatest(m.self_consumed_kwh, 0.0)          as total_consumption_kwh
from {{ source('rec_metering_gold', 'meters_data_15m') }} m
where m.device_id in (select device_id from active)
