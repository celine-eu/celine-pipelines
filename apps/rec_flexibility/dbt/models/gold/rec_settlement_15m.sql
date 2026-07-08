{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'ts',
      'device_id',
      'consumption_kwh',
      'grid_import_kwh',
      'grid_export_kwh',
      'total_consumption_kwh',
      'window_start',
      'window_end',
      'ts_date',
      'window_estimated_kwh',
      'window_reward_points_estimated',
    ]
  )
}}

-- Per-device 15-min settlement intervals, annotated with flexibility window context.
-- Source: rec_meters_15m (view over ds_dev_gold.meters_data_15m, kWh per bucket).
-- LEFT JOIN means every metered interval is present; window_start IS NOT NULL indicates
-- the interval falls inside a forecast flexibility window.
-- Used by rec_settlement_1h (hourly rollup) and consumed by flexibility-api for
-- per-commitment actual reward point computation (consumption_kwh × 10 points/kWh).
--
-- consumption_kwh remains grid import — its long-standing contract for
-- rec_settlement_1h / co2 / summary / flexibility-api. rec_meters_15m already
-- delivers kWh per 15-min bucket, so values pass through with NO unit conversion.
-- The v2 grid-based points migration additionally carries three explicit bases so
-- rec_settlement_points can pick the right reward numerator per device:
--   grid_import_kwh        = consumption_kwh (energy drawn from grid)
--   grid_export_kwh        = production_kwh  (energy fed to grid)
--   total_consumption_kwh  = behind-meter total (import + self-consumed PV)
--
-- Note: virtual_consumption_kwh and ratio (Italian GSE allocation) are intentionally
-- absent here — they belong in rec_it. Settlement is based on actual device consumption.

with base as (
    -- DISTINCT ON (device_id, ts) guards against a 15-min interval matching more
    -- than one flexibility window (e.g. overlapping or adjacent windows in the
    -- 2-day refresh window). We keep the earliest matching window; NULLs sort last
    -- so base-consumption intervals (no window) are also handled correctly.
    select distinct on (v.device_id, v.ts)
        v.ts,
        v.device_id,
        -- rec_meters_15m values are already kWh per 15-min bucket: pass through
        -- unchanged. Any ×0.25 here would 4×-under-count every downstream figure.
        v.consumption_kwh,
        v.consumption_kwh               as grid_import_kwh,
        v.production_kwh                as grid_export_kwh,
        v.total_consumption_kwh,
        w.window_start,
        w.window_end,
        w.ts_date,
        w.estimated_kwh           as window_estimated_kwh,
        w.reward_points_estimated as window_reward_points_estimated
    from {{ ref('rec_meters_15m') }} v
    left join {{ ref('rec_flexibility_windows') }} w
        on  v.device_id = w.device_id
        and v.ts >= w.window_start
        and v.ts <  w.window_end

    {% if is_incremental() %}
    where v.ts >= date_trunc('day', now() - interval '2 days')
    {% endif %}

    order by v.device_id, v.ts, w.window_start nulls last
)

select
    md5(device_id || ts::text)    as _id,
    ts,
    device_id,
    consumption_kwh,
    grid_import_kwh,
    grid_export_kwh,
    total_consumption_kwh,
    window_start,
    window_end,
    ts_date,
    window_estimated_kwh,
    window_reward_points_estimated
from base
