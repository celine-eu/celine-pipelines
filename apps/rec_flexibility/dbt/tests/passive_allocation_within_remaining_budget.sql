-- Business invariant: the sum of passive allocated_kwh per window must never exceed
-- the remaining budget (community_kwh − committed_allocated_total).
--
-- Passive allocated_kwh per device per window is not directly stored, so we re-derive
-- it from rec_gamification_summary (total_allocated_kwh) and rec_settlement_15m
-- (to isolate per-window contributions). Instead, we assert at the daily level:
-- for passive devices, total_allocated_kwh ≤ sum(remaining_budget across their windows).
--
-- A non-empty result indicates passive devices received more points than the available
-- remaining solar surplus for that day.

with committed_allocated_per_window as (
    select
        period_start,
        period_end,
        sum(allocated_kwh) as committed_total
    from {{ ref('rec_commitment_settlement') }}
    where status in ('committed', 'settled')
    group by period_start, period_end
),

window_budget as (
    select distinct on (window_start, window_end)
        window_start,
        window_end,
        community_kwh
    from {{ ref('rec_flexibility_windows') }}
    order by window_start, window_end
),

remaining_per_window as (
    select
        wb.window_start,
        wb.window_end,
        greatest(0, wb.community_kwh - coalesce(ca.committed_total, 0)) as remaining_budget
    from window_budget wb
    left join committed_allocated_per_window ca
        on  ca.period_start = wb.window_start
        and ca.period_end   = wb.window_end
),

-- Total remaining budget available for passive devices per day
-- (sum across all windows that day)
daily_remaining as (
    select
        window_start::date as ts_date,
        sum(remaining_budget) as total_remaining
    from remaining_per_window
    group by window_start::date
)

select
    g.device_id,
    g.ts_date,
    g.total_allocated_kwh,
    dr.total_remaining
from {{ ref('rec_gamification_summary') }} g
join daily_remaining dr using (ts_date)
where g.committed = false
  and g.total_allocated_kwh > dr.total_remaining + 0.001
