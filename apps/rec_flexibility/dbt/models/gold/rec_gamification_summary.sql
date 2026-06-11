{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'ts_date',
      'device_id',
      'total_consumption_kwh',
      'total_allocated_kwh',
      'committed',
      'percentile_rank',
      'rank_position',
      'total_members'
    ]
  )
}}

-- Per-device daily ranking and budget-aware passive points allocation.
-- Source: rec_settlement_15m filtered to window intervals (window_start IS NOT NULL).
--
-- Two-tier allocation:
--   Committed (status in committed/settled on that date):
--     Priority access to the full community_kwh budget.
--     Their window consumption is excluded from the passive pool.
--     total_allocated_kwh = 0 here; points are authoritative in rec_commitment_settlement.
--   Passive (no active commitment on that date):
--     Proportional share of remaining_budget = community_kwh − committed_allocated_total.
--     If committed devices exhaust the budget, passive devices receive 0.
--     If passive_total ≤ remaining_budget, each passive device keeps its actual consumption.
--     If passive_total > remaining_budget, consumption is scaled proportionally.
--
-- Ranking (percentile_rank, rank_position) is based on total_consumption_kwh across all
-- devices in the same day, regardless of committed status, for leaderboard display.

with device_window_consumption as (
    -- Per-device, per-window actual consumption from 15-min intervals.
    select
        s.device_id,
        s.ts_date,
        s.window_start,
        s.window_end,
        sum(s.consumption_kwh) as window_consumption_kwh
    from {{ ref('rec_settlement_15m') }} s
    where s.window_start is not null

    {% if is_incremental() %}
    and s.ts >= date_trunc('day', now() - interval '2 days')
    {% endif %}

    group by s.device_id, s.ts_date, s.window_start, s.window_end
),

-- Devices holding an active commitment on each calendar date.
-- A device committed on a date is excluded from the passive pool for the whole day:
-- its points come from rec_commitment_settlement, not from here.
committed_on_date as (
    select distinct
        device_id,
        committed_at::date as ts_date
    from {{ ref('silver_flexibility_commitments') }}
    where status in ('committed', 'settled')

    {% if is_incremental() %}
    and committed_at::date >= (current_date - interval '2 days')::date
    {% endif %}
),

-- Total allocated_kwh consumed by committed devices per window.
-- Already proportionally capped at community_kwh by rec_commitment_settlement.
committed_allocated_per_window as (
    select
        period_start,
        period_end,
        sum(allocated_kwh) as committed_allocated_total
    from {{ ref('rec_commitment_settlement') }}
    where status in ('committed', 'settled')
    group by period_start, period_end
),

-- Community solar budget per window (identical across all devices in a window).
window_budget as (
    select distinct on (window_start, window_end)
        window_start,
        window_end,
        community_kwh
    from {{ ref('rec_flexibility_windows') }}
    order by window_start, window_end
),

-- Remaining budget available for passive devices after committed allocation.
-- remaining_budget = max(0, community_kwh − committed_allocated_total).
-- Windows with no budget row (e.g. DSO flex) yield remaining_budget = 0.
window_remaining as (
    select
        wb.window_start,
        wb.window_end,
        greatest(
            0,
            wb.community_kwh - coalesce(ca.committed_allocated_total, 0)
        ) as remaining_budget
    from window_budget wb
    left join committed_allocated_per_window ca
        on  ca.period_start = wb.window_start
        and ca.period_end   = wb.window_end
),

-- Passive device consumption per window (devices without an active commitment that day).
passive_window as (
    select
        dw.device_id,
        dw.ts_date,
        dw.window_start,
        dw.window_end,
        dw.window_consumption_kwh,
        wr.remaining_budget
    from device_window_consumption dw
    left join committed_on_date cod
        on  cod.device_id = dw.device_id
        and cod.ts_date   = dw.ts_date
    left join window_remaining wr
        on  wr.window_start = dw.window_start
        and wr.window_end   = dw.window_end
    where cod.device_id is null   -- exclude committed devices
),

-- Total passive consumption per window — denominator for proportional capping.
passive_window_totals as (
    select
        window_start,
        window_end,
        sum(window_consumption_kwh) as passive_total_kwh
    from passive_window
    group by window_start, window_end
),

-- Allocate passive consumption within remaining budget per window.
--   remaining = 0 or null → 0 kWh (committed consumed all available solar surplus)
--   passive_total ≤ remaining  → full actual consumption
--   passive_total > remaining  → proportional share of remaining
passive_window_allocated as (
    select
        p.device_id,
        p.ts_date,
        p.window_consumption_kwh,
        case
            when p.remaining_budget is null or p.remaining_budget = 0
                then 0::numeric
            when pt.passive_total_kwh <= p.remaining_budget
                then p.window_consumption_kwh
            else
                round(
                    (p.window_consumption_kwh / nullif(pt.passive_total_kwh, 0) * p.remaining_budget)::numeric,
                    4
                )
        end as allocated_kwh
    from passive_window p
    left join passive_window_totals pt
        on  pt.window_start = p.window_start
        and pt.window_end   = p.window_end
),

-- Committed devices: daily aggregate for ranking context.
-- total_allocated_kwh = 0 — their points are in rec_commitment_settlement.
committed_daily as (
    select
        dw.device_id,
        dw.ts_date,
        sum(dw.window_consumption_kwh) as total_consumption_kwh,
        0::numeric                     as total_allocated_kwh,
        true                           as committed
    from device_window_consumption dw
    join committed_on_date cod
        on  cod.device_id = dw.device_id
        and cod.ts_date   = dw.ts_date
    group by dw.device_id, dw.ts_date
),

-- Passive devices: daily aggregate with budget-aware allocation.
passive_daily as (
    select
        device_id,
        ts_date,
        sum(window_consumption_kwh) as total_consumption_kwh,
        sum(allocated_kwh)          as total_allocated_kwh,
        false                       as committed
    from passive_window_allocated
    group by device_id, ts_date
),

daily as (
    select * from committed_daily
    union all
    select * from passive_daily
)

select
    md5(device_id || ts_date::text)      as _id,
    device_id,
    ts_date,
    total_consumption_kwh,
    total_allocated_kwh,
    committed,
    percent_rank() over (
        partition by ts_date
        order by total_consumption_kwh
    )                                    as percentile_rank,
    rank() over (
        partition by ts_date
        order by total_consumption_kwh desc
    )                                    as rank_position,
    count(*) over (partition by ts_date) as total_members
from daily
