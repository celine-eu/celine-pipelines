{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    full_refresh=false,
    tags=['rec_flexibility', 'fair']
  )
}}

-- ACCUMULATE-ONLY (full_refresh=false, like rec_flexibility_windows_community): past seasons
-- are frozen rows; a --full-refresh must never rebuild them from whatever weekly history
-- happens to be present. Drop the table by hand if it really has to be recreated.
-- Fair gamification: season standings per device for the three boards
-- (spec §4.5, design 2026-10-09). One row per (device, season); the current season is
-- merged on every run, past seasons stay frozen (spec §6: past seasons are not recomputed).
--
-- Seasons are the calendar blocks of rec_season_start (season_anchor_date, season_months);
-- a week belongs to the season containing its Monday. elapsed_weeks = season weeks
-- completed before as_of; required_weeks = ceil(elapsed / 2) (spec §4.6 "metà delle
-- settimane trascorse").
-- Boards use MEANS over the weeks that carry their inputs: Virtuosi = mean Level (tie-break
-- Σ Actions, then weeks at 100); In crescita = mean Growth (tie-break Σ Actions);
-- Generale = mean of the weekly Generale input + mean weekly Actions (Σ Actions / elapsed
-- weeks, 2026-11-05). The weekly input is w·L + (1−w)·G when both exist, and L alone when
-- Growth is undefined because the night import is already near zero
-- (growth_reason 'night_already_zero', 2026-11-05); weeks_with_both counts those weeks
-- (the app shows it as the Generale's week count). A board ranks a device
-- only when it is rankable (in app, household/public_entity, not quarantined, not silent)
-- and has at least required_weeks weeks with that board's inputs. Ranks are per
-- (community, season, category); unranked rows keep a rank_reason and null ranks.
-- season_points = Σ weekly_points (personal milestones: levels, badges).

{% set as_of = ("'" ~ var('fair_as_of') ~ "'::date") if var('fair_as_of', '') else 'current_date' %}
{% set months = var('season_months') %}

with seasons as (
    select distinct season_start from {{ ref('rec_fair_weekly_scores') }}
    union
    select {{ rec_season_start(as_of) }}
),
season_weeks as (
    select s.season_start,
           (s.season_start + interval '{{ months }} months')::date as season_end,
           w.week_start::date as week_start
    from seasons s
    cross join lateral generate_series(
        (s.season_start + ((8 - extract(isodow from s.season_start)::int) % 7))::date,
        (s.season_start + interval '{{ months }} months')::date - 1,
        interval '7 days'
    ) as w(week_start)
),
season_meta as (
    select season_start, season_end,
           count(*) filter (where week_start + 7 <= {{ as_of }}) as elapsed_weeks
    from season_weeks
    group by season_start, season_end
),
universe as (
    select k.device_id, k.community_id, k.category, k.pv_class, k.ref_group,
           k.is_in_app, k.is_quarantined, k.is_silent
    from {{ ref('rec_fair_device_class') }} k
    where k.week_start = (select max(week_start) from {{ ref('rec_fair_device_class') }})
),
weekly as (
    select sc.*,
           case when sc.level_points is not null and sc.growth_points is not null
                then {{ var('fair_general_weight_level') }} * sc.level_points
                     + (1 - {{ var('fair_general_weight_level') }}) * sc.growth_points
                when sc.level_points is not null and sc.growth_reason = 'night_already_zero'
                then sc.level_points
           end as general_week
    from {{ ref('rec_fair_weekly_scores') }} sc
),
per_device as (
    select sc.device_id, sc.community_id, sc.season_start,
           count(*) filter (where sc.is_valid_week)                                   as valid_weeks,
           count(sc.level_points)                                                     as weeks_with_level,
           count(sc.growth_points)                                                    as weeks_with_growth,
           count(sc.general_week)                                                     as weeks_with_both,
           avg(sc.level_points)                                                       as mean_level,
           avg(sc.growth_points)                                                      as mean_growth,
           avg(sc.general_week)                                                       as mean_general,
           sum(sc.actions_points)                                                     as actions_sum,
           count(*) filter (where sc.level_points >= 100)                             as weeks_at_100,
           sum(sc.weekly_points)                                                      as season_points,
           max(sc.week_start) filter (where sc.is_valid_week)                         as last_valid_week_start
    from weekly sc
    group by sc.device_id, sc.community_id, sc.season_start
),
joined as (
    select u.device_id, u.community_id, sm.season_start, sm.season_end, sm.elapsed_weeks,
           ceil(sm.elapsed_weeks / 2.0)::int                       as required_weeks,
           u.category, u.pv_class, u.ref_group                     as ref_group_effective,
           coalesce(p.valid_weeks, 0)                              as valid_weeks,
           coalesce(p.weeks_with_level, 0)                         as weeks_with_level,
           coalesce(p.weeks_with_growth, 0)                        as weeks_with_growth,
           coalesce(p.weeks_with_both, 0)                          as weeks_with_both,
           round(p.mean_level::numeric, 6)::double precision       as mean_level,
           round(p.mean_growth::numeric, 6)::double precision      as mean_growth,
           round(p.mean_general::numeric, 6)::double precision     as mean_general,
           coalesce(p.actions_sum, 0)                              as actions_sum,
           coalesce(p.weeks_at_100, 0)                             as weeks_at_100,
           coalesce(p.season_points, 0)                            as season_points,
           p.last_valid_week_start,
           case
               when sm.elapsed_weeks = 0                                      then 'first_week'
               when u.is_quarantined                                          then 'quarantined'
               when not u.is_in_app                                           then 'not_in_app'
               when u.category not in ('household', 'public_entity')          then 'category_not_ranked'
               when u.is_silent                                               then 'silent'
               when coalesce(p.valid_weeks, 0) < ceil(sm.elapsed_weeks / 2.0) then 'min_weeks'
               else 'ranked'
           end                                                     as rank_reason,
           (sm.season_start = {{ rec_season_start(as_of) }})       as is_current_season
    from universe u
    cross join season_meta sm
    left join per_device p
      on p.device_id = u.device_id and p.community_id = u.community_id and p.season_start = sm.season_start
),
scored as (
    select j.*,
           (j.rank_reason = 'ranked')                                                              as is_ranked,
           case when j.rank_reason = 'ranked' and j.weeks_with_level  >= greatest(j.required_weeks, 1) then j.mean_level end as virtuosi_score,
           case when j.rank_reason = 'ranked' and j.weeks_with_growth >= greatest(j.required_weeks, 1) then j.mean_growth end as growth_score,
           case when j.rank_reason = 'ranked' and j.weeks_with_both   >= greatest(j.required_weeks, 1)
                then round((j.mean_general + j.actions_sum::numeric / nullif(j.elapsed_weeks, 0))::numeric, 6)::double precision end as general_score
    from joined j
)
select
    md5(s.device_id || s.season_start::text) as _id,
    s.*,
    case when s.virtuosi_score is not null then
        rank() over (partition by s.community_id, s.season_start, s.category, (s.virtuosi_score is not null)
                     order by s.virtuosi_score desc, s.actions_sum desc, s.weeks_at_100 desc) end   as virtuosi_rank,
    case when s.growth_score is not null then
        rank() over (partition by s.community_id, s.season_start, s.category, (s.growth_score is not null)
                     order by s.growth_score desc, s.actions_sum desc) end                          as growth_rank,
    case when s.general_score is not null then
        rank() over (partition by s.community_id, s.season_start, s.category, (s.general_score is not null)
                     order by s.general_score desc) end                                             as general_rank,
    count(*) filter (where s.virtuosi_score is not null) over (partition by s.community_id, s.season_start, s.category) as total_ranked_virtuosi,
    count(*) filter (where s.growth_score   is not null) over (partition by s.community_id, s.season_start, s.category) as total_ranked_growth,
    count(*) filter (where s.general_score  is not null) over (partition by s.community_id, s.season_start, s.category) as total_ranked_general
from scored s
{% if is_incremental() %}
where s.is_current_season
{% endif %}
