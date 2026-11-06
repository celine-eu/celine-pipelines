{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['rec_flexibility', 'fair']
  )
}}

-- Fair gamification: Level, Growth and Actions points per device-week
-- (spec §4.3–§4.4, design 2026-10-09), with the reason codes and numbers the app renders
-- as "sei qui perché…".
--
-- Percentile (Hazen-like) within (community, week, ref_group_effective) over valid,
-- non-quarantined members of the playing categories (household, public_entity; 2026-11-05:
-- businesses and 'unknown' get no percentiles, reason 'not_compared'): pct = 100 × ((n − (rank_asc − 1) − cnt_eq) + 0.5 × (cnt_eq − 1)) / (n − 1),
-- null when n < 2. Level points = least(100, pct / fair_saturation_pct × 100).
-- Growth: delta minus the group median (seasonal correction), lower = better, points = pct.
-- Actions: least(cap, kept × fair_points_per_kept + surplus hours × fair_surplus_points_per_hour).
-- weekly_points = coalesce(L, 0) + coalesce(G, 0) + A (personal milestones sum these).
-- flag_import_rise (F1): total import > (1 + fair_import_rise_pct) × own 8-week median while
-- the night share fell — flag only, no points effect.

with m as (
    select mm.* from {{ ref('rec_fair_weekly_metrics') }} mm
),
eligible as (
    select * from m where is_valid_week and not is_quarantined and category in ('household', 'public_entity')
),
lvl as (
    select device_id, week_start,
           count(*) over g                                    as n,
           rank() over (g order by level_metric)              as r_asc,
           count(*) over (partition by community_id, week_start, ref_group_effective, level_metric) as cnt_eq
    from eligible
    where level_metric is not null
    window g as (partition by community_id, week_start, ref_group_effective)
),
lvl_pct as (
    select device_id, week_start,
           case when n < 2 then null
                else 100.0 * ((n - (r_asc - 1) - cnt_eq) + 0.5 * (cnt_eq - 1)) / (n - 1) end as level_pct
    from lvl
),
gmed as (
    select community_id, week_start, ref_group_effective,
           percentile_cont(0.5) within group (order by growth_delta) as growth_group_median
    from eligible
    where growth_delta is not null
    group by community_id, week_start, ref_group_effective
),
grw as (
    select e.device_id, e.week_start,
           e.growth_delta - g.growth_group_median              as growth_delta_adj,
           count(*) over w                                     as n,
           rank() over (w order by (e.growth_delta - g.growth_group_median)) as r_asc,
           count(*) over (partition by e.community_id, e.week_start, e.ref_group_effective, (e.growth_delta - g.growth_group_median)) as cnt_eq
    from eligible e
    join gmed g using (community_id, week_start, ref_group_effective)
    where e.growth_delta is not null
    window w as (partition by e.community_id, e.week_start, e.ref_group_effective)
),
grw_pct as (
    select device_id, week_start, growth_delta_adj,
           case when n < 2 then null
                else 100.0 * ((n - (r_asc - 1) - cnt_eq) + 0.5 * (cnt_eq - 1)) / (n - 1) end as growth_pct
    from grw
),
acts as (
    select device_id, week_start,
           count(*) filter (where is_kept)     as kept_count,
           sum(points)                         as kept_points,
           sum(surplus_hours_kept)             as surplus_hours,
           sum(surplus_points)                 as surplus_points,
           count(*)                            as commitments
    from {{ ref('rec_fair_commitment_outcomes') }}
    group by device_id, week_start
),
scored as (
    select
        m.*,
        round(lp.level_pct::numeric, 6)::double precision                          as level_pct,
        case when lp.level_pct is null then null
             else round(least(100.0, lp.level_pct / {{ var('fair_saturation_pct') }} * 100.0)::numeric, 6)::double precision end as level_points,
        gm.growth_group_median,
        round(gp.growth_delta_adj::numeric, 6)::double precision                   as growth_delta_adj,
        round(gp.growth_pct::numeric, 6)::double precision                         as growth_pct,
        coalesce(a.kept_count, 0)                                                  as kept_count,
        coalesce(a.kept_points, 0)                                                 as kept_points,
        coalesce(a.surplus_hours, 0)                                               as surplus_hours,
        coalesce(a.surplus_points, 0)                                              as surplus_points,
        coalesce(a.commitments, 0)                                                 as commitments
    from m
    left join lvl_pct lp on lp.device_id = m.device_id and lp.week_start = m.week_start
    left join grw_pct gp on gp.device_id = m.device_id and gp.week_start = m.week_start
    left join gmed gm on gm.community_id = m.community_id and gm.week_start = m.week_start and gm.ref_group_effective = m.ref_group_effective
    left join acts a on a.device_id = m.device_id and a.week_start = m.week_start
)
select
    md5(device_id || week_start::text) as _id,
    device_id, community_id, week_start, season_start, is_valid_week, is_rankable,
    pv_class, category, ref_group, ref_group_effective, ref_group_n,
    night_share, night_kwh_h, cloudy_valid_days, level_metric, level_source, level_carry_weeks,
    level_pct, level_points,
    case when not is_valid_week then 'no_valid_week'
         when category not in ('household', 'public_entity') then 'not_compared'
         when level_pct >= {{ var('fair_saturation_pct') }} then 'saturated'
         else level_source end                                                     as level_reason,
    growth_n, growth_delta, growth_group_median, growth_delta_adj, growth_pct,
    growth_pct                                                                     as growth_points,
    case when not is_valid_week then 'no_valid_week'
         when category not in ('household', 'public_entity') then 'not_compared'
         when growth_n is null then 'need_weeks'
         when growth_delta is null then 'night_already_zero'
         when growth_delta_adj < -0.05 then 'improving'
         when growth_delta_adj > 0.05 then 'worsening'
         else 'stable' end                                                         as growth_reason,
    kept_count, kept_points, surplus_hours, surplus_points,
    least({{ var('fair_actions_weekly_cap') }}, kept_points + surplus_points)      as actions_points,
    case when commitments = 0 then 'no_commitments'
         when kept_points + surplus_points > {{ var('fair_actions_weekly_cap') }} then 'capped'
         when kept_count > 0 then 'kept'
         else 'none' end                                                           as actions_reason,
    round((coalesce(level_points, 0) + coalesce(growth_pct, 0)
           + least({{ var('fair_actions_weekly_cap') }}, kept_points + surplus_points))::numeric, 6)::double precision as weekly_points,
    import_median_8w,
    round((import_kwh / nullif(import_median_8w, 0) - 1)::numeric, 6)::double precision as import_rise_pct,
    night_share_prev,
    coalesce(import_kwh > (1 + {{ var('fair_import_rise_pct') }}) * import_median_8w
             and night_share < night_share_prev, false)                            as flag_import_rise
from scored
