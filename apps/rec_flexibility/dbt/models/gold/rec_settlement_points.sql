{{
    config(
        materialized='incremental',
        unique_key='_id',
        incremental_strategy='merge',
        merge_update_columns=[
            'ts', 'device_id', 'consumption_kwh', 'baseline_kwh',
            'is_m1_only', 'grid_import_kwh', 'grid_export_kwh',
            'effort_ratio', 'effort_multiplier',
            'comm_grid_export_kwh', 'comm_grid_import_kwh',
            'is_surplus_interval', 'has_production',
            'log_scaled_points', 'effort_adjusted_points',
            'pool_share_points', 'settlement_points'
        ]
    )
}}

-- v2 grid-based settlement points.
--
-- Two distinct quantities, kept separate on purpose:
--   1. Community surplus (grid-based): comm_grid_export_kwh (community forecast 'actual'
--      grid export) vs comm_grid_import_kwh (Σ grid_import_kwh). Drives is_surplus_interval,
--      has_production and the deficit pool size. This is pure grid accounting.
--   2. Per-device reward numerator (consumption_kwh): the energy the device is rewarded
--      for. For M1+M2 devices this is behind-meter total_consumption_kwh; for M1-only
--      devices (no PV decomposition) it is the consumption proxy
--          grid_import + max(0, grid_export_median − grid_export)
--      which credits a reduction of export below the device's typical level as
--      self-consumption. The matching settlement baseline_kwh is computed on the SAME
--      basis (baseline_task), so effort_ratio is a consistent, unitless ratio — this
--      fixes the v1 mixed-basis bug (grid-import numerator vs total-consumption baseline).
--
-- Unit contract: all *_kwh are kWh per 15-min bucket (silver kW × 0.25; baseline task
-- applies × 0.25 at ingest). total_meters_forecast.production_kwh is hourly → ÷4.

with intervals as (
    select
        s.ts,
        s.device_id,
        s.grid_import_kwh,
        s.grid_export_kwh,
        s.total_consumption_kwh,
        extract(hour from s.ts) * 4 + extract(minute from s.ts)::int / 15 as slot,
        extract(dow from s.ts) between 1 and 5 as is_weekday
    from {{ ref('rec_settlement_15m') }} s
    {% if is_incremental() %}
    where s.ts >= date_trunc('day', now() - interval '2 days')
    {% endif %}
),
classed as (
    -- v2 reward basis per device: M1+M2 → behind-meter total; M1-only → consumption proxy.
    select
        i.*,
        coalesce(dc.is_m1_only, false)  as is_m1_only,
        coalesce(gem.baseline_kwh, 0.0) as grid_export_median_kwh,
        case
            when coalesce(dc.is_m1_only, false)
                then i.grid_import_kwh
                     + greatest(coalesce(gem.baseline_kwh, 0.0) - i.grid_export_kwh, 0.0)
            else i.total_consumption_kwh
        end as consumption_kwh
    from intervals i
    left join {{ ref('rec_device_class') }} dc
      on dc.device_id = i.device_id
    left join {{ ref('rec_device_baselines') }} gem
      on gem.device_id = i.device_id
     and gem.slot = i.slot
     and gem.is_weekday = i.is_weekday
     and gem.baseline_type = 'grid_export_median'
),
with_baseline as (
    select
        c.*,
        coalesce(b.baseline_kwh, 0.025) as baseline_kwh   -- 0.025 kWh/15min ~= 0.1 kW floor
    from classed c
    left join {{ ref('rec_device_baselines') }} b
      on b.device_id = c.device_id
     and b.slot = c.slot
     and b.is_weekday = c.is_weekday
     and b.baseline_type = 'settlement'
),
community as (
    -- Community grid import = Σ per-device grid import (NOT the reward basis). Pairs with
    -- the forecast grid export below to define the grid-based surplus.
    select
        ts,
        sum(grid_import_kwh) as comm_grid_import_kwh
    from intervals
    group by ts
),
production as (
    -- total_meters_forecast.production_kwh is hourly kWh (forecast tables use correct
    -- _kwh suffixes). Split per 15-min slot = hourly / 4. period='actual' gives the
    -- realised community grid export; forecast rows are ignored here.
    select
        date_trunc('hour', t.timestamp::timestamp) as ts_hour,
        avg(t.production_kwh) / 4.0 as comm_grid_export_kwh_15m
    from {{ source('meters_gold', 'total_meters_forecast') }} t
    where t.period = 'actual'
    group by date_trunc('hour', t.timestamp::timestamp)
),
joined as (
    select
        wb.ts,
        wb.device_id,
        wb.is_m1_only,
        wb.grid_import_kwh,
        wb.grid_export_kwh,
        wb.consumption_kwh,
        wb.baseline_kwh,
        coalesce(p.comm_grid_export_kwh_15m, 0) as comm_grid_export_kwh,
        c.comm_grid_import_kwh,
        case
            when coalesce(p.comm_grid_export_kwh_15m, 0) >= c.comm_grid_import_kwh then 1
            else 0
        end as is_surplus_interval,
        case
            when coalesce(p.comm_grid_export_kwh_15m, 0) > 0 then 1 else 0
        end as has_production,
        case
            when wb.baseline_kwh < 0.025 then 1.0
            else wb.consumption_kwh / nullif(wb.baseline_kwh, 0)
        end as effort_ratio
    from with_baseline wb
    left join community c using (ts)
    left join production p on p.ts_hour = date_trunc('hour', wb.ts)
),
with_effort as (
    select
        j.*,
        case
            when effort_ratio >= 1.50 then 2.00
            when effort_ratio >= 1.25 then 1.50
            when effort_ratio >= 1.10 then 1.00
            when effort_ratio >= 1.00 then 0.50
            else 0.25
        end as effort_multiplier
    from joined j
),
points_calc as (
    select
        we.*,
        ln(1 + we.consumption_kwh) as log_consumption,
        ln(1 + we.consumption_kwh) * we.effort_multiplier * 10 as log_scaled_points,
        case
            when we.is_surplus_interval = 1
                then ln(1 + we.consumption_kwh) * we.effort_multiplier * 10
            else 0
        end as effort_adjusted_points,
        sum(ln(1 + we.consumption_kwh) * we.effort_multiplier)
            over (partition by we.ts) as ts_log_weighted_total
    from with_effort we
),
deficit_share as (
    select
        pc.*,
        case
            when pc.is_surplus_interval = 0
                 and pc.has_production = 1
                 and pc.ts_log_weighted_total > 0
            then (pc.comm_grid_export_kwh * 10)
                 * (ln(1 + pc.consumption_kwh) * pc.effort_multiplier
                    / pc.ts_log_weighted_total)
            else 0
        end as pool_share_points
    from points_calc pc
)
select
    md5(device_id || ts::text) as _id,
    ts,
    device_id,
    consumption_kwh,
    baseline_kwh,
    is_m1_only,
    grid_import_kwh,
    grid_export_kwh,
    effort_ratio,
    effort_multiplier,
    comm_grid_export_kwh,
    comm_grid_import_kwh,
    is_surplus_interval,
    has_production,
    log_scaled_points,
    effort_adjusted_points,
    pool_share_points,
    effort_adjusted_points + pool_share_points as settlement_points
from deficit_share
