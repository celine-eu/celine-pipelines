-- Business invariant: the sum of allocated_kwh across all committed/settled devices
-- sharing the same window must never exceed that window's community_kwh budget.
--
-- A non-empty result means the proportional redistribution logic in
-- rec_commitment_settlement has over-allocated the solar surplus for at least one window.

with committed_per_window as (
    select
        period_start,
        period_end,
        sum(allocated_kwh) as total_allocated
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
)

select
    cp.period_start,
    cp.period_end,
    cp.total_allocated,
    wb.community_kwh
from committed_per_window cp
join window_budget wb
    on  wb.window_start = cp.period_start
    and wb.window_end   = cp.period_end
-- Allow a small float tolerance for rounding in proportional redistribution
where cp.total_allocated > wb.community_kwh + 0.001
