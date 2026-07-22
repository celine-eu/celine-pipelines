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
--   season_base_points, season_bonus_points, season_points, season_rank, total_members
-- The frontend shows the row WHERE is_current_season is true. season_end is exclusive
-- (start of the next season); it is the ONLY place season length is materialized —
-- downstream consumers must derive countdowns from it, never hardcode the length.
--
-- Fleet-complete current season: the current season is built from the rec_active_devices
-- seed LEFT JOINed to earned aggregates, so a device added to the fleet appears the same
-- run with 0 points, ranked (tied) last. Past seasons stay earned-rows-only (no
-- retroactive zero-rows); their total_members counts devices that actually earned.
--
-- PRIVATE (never shown to the participant) — lifetime cumulative since the anchor date,
-- carried for analytics/audit only. Consumers MUST NOT expose these:
--   alltime_base_points, alltime_bonus_points
-- All-time totals are computed over EARNED rows only, so they are unaffected by the
-- current-season zero-fill and survive a device's removal from the fleet.
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
current_season_universe as (
    select
        device_id,
        {{ rec_season_start('current_date') }} as season_start
    from {{ ref('rec_active_devices') }}
),
seasons_complete as (
    -- past seasons: earned rows as-is
    select
        device_id,
        season_start,
        season_base_points,
        season_bonus_points,
        season_points
    from per_season
    where season_start <> {{ rec_season_start('current_date') }}
    union all
    -- current season: every fleet device, 0-filled
    select
        u.device_id,
        u.season_start,
        coalesce(ps.season_base_points, 0) as season_base_points,
        coalesce(ps.season_bonus_points, 0) as season_bonus_points,
        coalesce(ps.season_points, 0)       as season_points
    from current_season_universe u
    left join per_season ps
      on ps.device_id = u.device_id
     and ps.season_start = u.season_start
),
alltime as (
    -- Earned rows only: independent of the fleet union above.
    select
        device_id,
        sum(season_base_points)  as alltime_base_points,
        sum(season_bonus_points) as alltime_bonus_points
    from per_season
    group by device_id
)
select
    md5(s.device_id || s.season_start::text)          as _id,
    s.device_id,
    s.season_start,
    (s.season_start + interval '{{ var("season_months") }} months')::date as season_end,
    -- shown
    s.season_base_points,
    s.season_bonus_points,
    s.season_points,
    rank() over (partition by s.season_start order by s.season_points desc) as season_rank,
    count(*) over (partition by s.season_start)                             as total_members,
    -- private (lifetime, never reset)
    coalesce(a.alltime_base_points, 0)  as alltime_base_points,
    coalesce(a.alltime_bonus_points, 0) as alltime_bonus_points,
    -- the season containing "today" — the row the leaderboard displays
    (s.season_start = {{ rec_season_start('current_date') }}) as is_current_season
from seasons_complete s
left join alltime a
  on a.device_id = s.device_id
