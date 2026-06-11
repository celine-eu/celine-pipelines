{{
  config(
    materialized='incremental',
    unique_key='commitment_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'status',
      'actual_kwh',
      'allocated_kwh',
      'reward_points_actual',
      'adherence_ratio'
    ]
  )
}}

-- Three-layer scoring (2026-04-17): reward_points_actual is no longer
-- round(allocated_kwh * 10). It is the sum of the device's Layer-1 settlement points
-- (rec_settlement_points) over [period_start, period_end) PLUS the device's Layer-2
-- flexibility bonus (rec_flexibility_bonus) for the matching window. allocated_kwh and
-- actual_kwh are unchanged (kWh quantities, not points) and remain the basis for the
-- kWh-based budget tracking. Both sides of adherence_ratio (actual and estimated) now
-- use the log-scaled formula (estimated was rewritten in Task 0 Step 0.8), so the ratio
-- remains a coherent delivery measure.

-- Per-commitment settlement: joins commitment records with actual hourly consumption
-- during the committed window to compute actual_kwh and adherence.
--
-- actual_kwh: raw device consumption during the committed window.
--
-- allocated_kwh: proportional share of the window's available budget.
--   For solar_overproduction: budget = community_kwh from rec_flexibility_windows.
--   When multiple devices commit to the same suggestion (window), their combined
--   actual_kwh is capped at community_kwh and redistributed proportionally so the
--   total distributed kWh never exceeds the available solar surplus.
--   Falls back to actual_kwh when no budget row exists (DSO flex — budget TBD).
--
-- reward_points_actual is computed from allocated_kwh (10 points/kWh) and is
-- authoritative over the API mirror, reflecting the normalised settlement.
--
-- adherence_ratio = reward_points_actual / reward_points_estimated — how much of
-- the committed flexibility was actually delivered. NULL until settled.
--
-- Rejected and cancelled commitments are included (actual_kwh = 0) so acceptance
-- rate and no-show analytics are available in one place.

with commitments as (
    select *
    from {{ ref('silver_flexibility_commitments') }}

    {% if is_incremental() %}
    -- Re-process commitments updated in the last 2 days to catch status transitions
    -- (committed → settled) and new commitments in the sliding window.
    where last_updated >= date_trunc('day', now() - interval '2 days')
    {% endif %}
),

settlement as (
    select
        device_id,
        ts,
        consumption_kwh
    from {{ ref('rec_settlement_1h') }}
    where window_start is not null
),

actuals as (
    select
        c.commitment_id,
        coalesce(sum(s.consumption_kwh), 0) as actual_kwh
    from commitments c
    left join settlement s
        on  s.device_id = c.device_id
        and s.ts >= c.period_start
        and s.ts <  c.period_end
    group by c.commitment_id
),

-- Community solar budget per window. community_kwh is identical for every device
-- in the same window; collapse to one row per (window_start, window_end).
window_budget as (
    select distinct on (window_start, window_end)
        window_start,
        window_end,
        community_kwh
    from {{ ref('rec_flexibility_windows') }}
    order by window_start, window_end
),

-- Total actual_kwh delivered by all active commitments sharing the same suggestion.
-- This is the denominator for proportional redistribution.
-- Only committed/settled statuses contribute — rejected/cancelled add no delivery.
suggestion_actuals as (
    select
        c.suggestion_id,
        sum(a.actual_kwh) as total_actual_kwh
    from commitments c
    join actuals a using (commitment_id)
    where c.status in ('committed', 'settled')
    group by c.suggestion_id
),

-- Proportional allocation: when participants collectively exceed the window budget,
-- each device's share is scaled down by (device_actual / total_actual) * budget.
-- Three cases:
--   1. Rejected/cancelled → 0 kWh
--   2. No budget row (DSO flex, window outside refresh range) → use raw actual_kwh
--   3. Under budget → use raw actual_kwh (no adjustment)
--   4. Over budget → proportional share of community_kwh
allocated as (
    select
        c.commitment_id,
        a.actual_kwh,
        case
            when c.status not in ('committed', 'settled')
                then 0::numeric
            when wb.community_kwh is null or sa.total_actual_kwh is null
                then a.actual_kwh
            when sa.total_actual_kwh <= wb.community_kwh
                then a.actual_kwh
            else
                round(
                    (a.actual_kwh / nullif(sa.total_actual_kwh, 0) * wb.community_kwh)::numeric,
                    4
                )
        end as allocated_kwh
    from commitments c
    join actuals a using (commitment_id)
    left join suggestion_actuals sa
        on  sa.suggestion_id = c.suggestion_id
    left join window_budget wb
        on  wb.window_start = c.period_start
        and wb.window_end   = c.period_end
),

-- Layer 1: sum of per-15-min settlement points over the committed window.
-- Earned by every device regardless of commitment status; but for a committed
-- device, this is the device's own L1 contribution during the window.
window_settlement as (
    select
        c.commitment_id,
        coalesce(sum(sp.settlement_points), 0) as layer1_points
    from commitments c
    left join {{ ref('rec_settlement_points') }} sp
      on  sp.device_id = c.device_id
     and  sp.ts >= c.period_start and sp.ts < c.period_end
    group by c.commitment_id
),

-- Layer 2: flexibility bonus for the matching (device, window). rec_flexibility_bonus
-- already enforces commitment-gating at the semi-join, so rows here only exist for
-- active commitments.
window_bonus as (
    select
        b.device_id,
        b.window_start,
        b.window_end,
        coalesce(sum(b.bonus_points), 0) as layer2_points
    from {{ ref('rec_flexibility_bonus') }} b
    group by b.device_id, b.window_start, b.window_end
)

select
    c.commitment_id,
    c.user_id,
    c.device_id,
    c.community_id,
    c.suggestion_type,
    c.period_start,
    c.period_end,
    c.status,
    c.reward_points_estimated,
    -- Authoritative: Layer-1 settlement points over the committed window + Layer-2 bonus.
    round(ws.layer1_points + coalesce(wb.layer2_points, 0))::int        as reward_points_actual,
    c.window_duration_hours,
    al.actual_kwh,
    al.allocated_kwh,
    case
        when c.status = 'settled' and c.reward_points_estimated > 0
        then (round(ws.layer1_points + coalesce(wb.layer2_points, 0))::int)::numeric
             / c.reward_points_estimated
        else null
    end                                                                  as adherence_ratio,
    c.committed_at,
    c.settled_at,
    c.last_updated
from commitments c
join allocated al using (commitment_id)
join window_settlement ws using (commitment_id)
left join window_bonus wb
       on wb.device_id     = c.device_id
      and wb.window_start  = c.period_start
      and wb.window_end    = c.period_end
