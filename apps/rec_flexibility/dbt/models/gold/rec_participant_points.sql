{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'ts_date',
      'season_start',
      'device_id',
      'daily_consumption_kwh',
      'daily_settlement_points',
      'daily_bonus_points',
      'daily_points'
    ]
  )
}}

-- Per-device daily base points.
-- Three-layer scoring (2026-04-17):
--   daily_settlement_points = sum of Layer-1 points across the day (every device).
--   daily_bonus_points      = sum of Layer-2 bonus across the day (committed only).
--   daily_points            = settlement + bonus.
-- daily_consumption_kwh is the raw window-consumption carried through from
-- rec_gamification_summary for display (no formula change).
--
-- Clients may still sum daily_points with FlexibilityCommitment.reward_points_actual
-- for cumulative totals, but the authoritative per-commitment figure now also
-- aggregates Layer 1 + Layer 2 inside rec_commitment_settlement (Task 13).

with settlement as (
    select
        device_id,
        ts::date as ts_date,
        sum(settlement_points) as total_settlement_points,
        sum(consumption_kwh)   as daily_consumption_kwh
    from {{ ref('rec_settlement_points') }}
    {% if is_incremental() %}
    where ts >= date_trunc('day', now() - interval '2 days')
    {% endif %}
    group by device_id, ts::date
),
bonus as (
    select
        device_id,
        date_trunc('day', window_start)::date as ts_date,
        sum(bonus_points) as daily_bonus_points
    from {{ ref('rec_flexibility_bonus') }}
    {% if is_incremental() %}
    where window_start >= date_trunc('day', now() - interval '2 days')
    {% endif %}
    group by device_id, date_trunc('day', window_start)::date
)
select
    md5(st.device_id || st.ts_date::text)                           as _id,
    st.device_id,
    st.ts_date,
    {{ rec_season_start('st.ts_date') }}                            as season_start,
    st.daily_consumption_kwh,
    coalesce(round(st.total_settlement_points)::int, 0)             as daily_settlement_points,
    coalesce(b.daily_bonus_points, 0)::int                          as daily_bonus_points,
      coalesce(round(st.total_settlement_points)::int, 0)
    + coalesce(b.daily_bonus_points, 0)::int                        as daily_points
from settlement st
left join bonus b using (device_id, ts_date)
