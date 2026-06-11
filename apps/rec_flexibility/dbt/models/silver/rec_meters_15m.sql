{{
    config(
        materialized='view',
        tags=['rec_flexibility']
    )
}}

-- Analysis-ready 15-min meter table derived from ds_dev_silver.meters_data.
-- Replaces every downstream read of the banned ds_dev_gold.meters_data_15m.
--
-- Columns remain in kW (average power over the 15-min bucket). To get 15-min kWh
-- use × 0.25; to get hourly kWh use mean(4 values).
--
-- self_consumed_kw is derived (not present at silver) exactly as in
-- gamification/data_loader.py::load_meters:
--     pv_production_kw = M2.production_kw + M2_2.production_kw   -- gross PV
--     grid_export_kw   = M1.production_kw                        -- export to grid
--     self_consumed_kw = clip(pv_production_kw − grid_export_kw, ≥0)
--     total_cons_kw    = M1.consumption_kw + self_consumed_kw

-- v2 fleet scope: restrict to the active 11-device fleet (seed kept in sync with
-- flexibility_config.yaml fleet.active_devices). All downstream models inherit it.
with active as (
    select device_id from {{ ref('rec_active_devices') }}
),
m1 as (
    select distinct on (device_id, ts)
        device_id,
        ts,
        consumption_kw,
        production_kw                   as grid_export_kw
    from {{ source('rec_metering_silver', 'meters_data') }}
    where meter_type = 'M1'
      and device_id in (select device_id from active)
    order by device_id, ts, _id desc
),
m2_sum as (
    select
        device_id,
        ts,
        sum(production_kw) as pv_production_kw
    from {{ source('rec_metering_silver', 'meters_data') }}
    where meter_type in ('M2', 'M2_2')
      and device_id in (select device_id from active)
    group by device_id, ts
)
select
    md5(m1.device_id || m1.ts::text)                                as _id,
    m1.device_id,
    m1.ts,
    m1.consumption_kw,
    m1.grid_export_kw                                                as production_kw,
    coalesce(m2.pv_production_kw, 0.0)                               as pv_production_kw,
    greatest(coalesce(m2.pv_production_kw, 0.0) - m1.grid_export_kw, 0.0)
                                                                     as self_consumed_kw,
    m1.consumption_kw
        + greatest(coalesce(m2.pv_production_kw, 0.0) - m1.grid_export_kw, 0.0)
                                                                     as total_consumption_kw
from m1
left join m2_sum m2 using (device_id, ts)
