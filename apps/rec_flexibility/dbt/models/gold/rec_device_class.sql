{{
    config(
        materialized='view',
        tags=['rec_flexibility']
    )
}}

-- Dynamic per-device classification for the v2 grid-based points migration.
--
-- A device is M1-only (no behind-meter PV decomposition) iff it never reports any
-- M2/M2_2 production over its history, i.e. max(pv_production_kw) = 0. This is the
-- SQL equivalent of gamification/v2 data_loader.identify_m1_only_devices
-- (has_m2 == False) and replaces the hardcoded 3-device list. M1-only devices use
-- the consumption proxy (grid_import + max(0, grid_export_median - grid_export))
-- for settlement instead of total_consumption_kw.

select
    device_id,
    (max(pv_production_kw) = 0.0) as is_m1_only
from {{ ref('rec_meters_15m') }}
group by device_id
