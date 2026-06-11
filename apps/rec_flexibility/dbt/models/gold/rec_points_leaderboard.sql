{{
  config(
    materialized='table',
    tags=['rec_flexibility', 'gamification']
  )
}}

-- Per-device, per-season points standings with periodic reset.
--
-- Grain: one row per (device_id, season_start). Seasons are calendar-aligned blocks of
-- `season_months` months anchored at `season_anchor_date` (dbt_project.yml vars, mirrored
-- in flexibility_config.yaml `season`).
--
-- DISPLAYED (resettable) — the points a participant sees. They accrue within a season and
-- return to 0 at each season boundary, because each season is a separate row:
--   season_base_points, season_bonus_points, season_points
-- The frontend shows the row WHERE is_current_season is true.
--
-- PRIVATE (never shown to the participant) — lifetime cumulative since the anchor date,
-- carried for analytics/audit only. Consumers MUST NOT expose these:
--   alltime_base_points, alltime_bonus_points
--
-- Materialized as a full table (not incremental): the all-time cumulative columns are
-- global sums and must be recomputed wholesale on every run to stay correct.

with per_season as (
    select
        device_id,
        season_start,
        sum(daily_settlement_points)::bigint as season_base_points,
        sum(daily_bonus_points)::bigint      as season_bonus_points,
        sum(daily_points)::bigint            as season_points
    from {{ ref('rec_participant_points') }}
    -- Program starts at the anchor date: any pre-anchor partial season (e.g. data that
    -- predates 2025-09-01) is excluded so the all-time totals are truly "since anchor".
    where season_start >= date_trunc('month', cast('{{ var("season_anchor_date") }}' as date))
    group by device_id, season_start
),
with_alltime as (
    select
        ps.*,
        -- Lifetime totals per device (same value on every one of the device's rows).
        sum(ps.season_base_points)  over (partition by ps.device_id) as alltime_base_points,
        sum(ps.season_bonus_points) over (partition by ps.device_id) as alltime_bonus_points
    from per_season ps
)
select
    md5(device_id || season_start::text)              as _id,
    device_id,
    season_start,
    -- shown
    season_base_points,
    season_bonus_points,
    season_points,
    -- private (lifetime, never reset)
    alltime_base_points,
    alltime_bonus_points,
    -- the season containing "today" — the row the leaderboard displays
    (season_start = {{ rec_season_start('current_date') }}) as is_current_season
from with_alltime
