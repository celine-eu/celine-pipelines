{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['rec_flexibility', 'fair']
  )
}}

-- Fair gamification: per device and completed ISO week, the inputs of Level and
-- Growth (spec §4.4, design 2026-10-09). Recomputed for the trailing
-- fair_recompute_weeks weeks each run (incremental merge), so carry-forward and
-- growth windows are evaluated inside the run with window functions.
--
-- Level metric: night import share (no_pv: all valid days; pv: cloudy valid days only,
-- needing fair_min_cloudy_days of them). A PV week without enough cloudy days carries
-- the last metric found within the previous fair_level_carry_max_weeks weeks
-- ('carry_forward'); an invalid week carries nothing but counts toward that distance.
-- Growth: n = min(4, floor(k/2)), k = valid weeks in the trailing fair_growth_lookback_weeks;
-- delta = (mean night kWh/h of the last n valid weeks − mean of the n before) / the latter,
-- undefined below fair_growth_prev_floor_kwh_h. Rounded to 6 decimals.
-- Reference group: category/pv_class; if ANY group of a PV class in the community-week
-- is smaller than fair_min_group_size, every member of that PV class is compared in
-- 'all/<pv_class>' (spec §4.3: the small group joins the other category).
-- Peers are only the playing categories, household and public_entity, in the app or not
-- (2026-11-05): businesses and 'unknown' devices are nobody's peers. They keep their own
-- ref_group and ref_group_n = 0, and are never merged.

{% set as_of = ("'" ~ var('fair_as_of') ~ "'::date") if var('fair_as_of', '') else 'current_date' %}
{# Weeks (re)computed this run: the trailing fair_recompute_weeks on an incremental run,
   every completed week since fair_history_start on a full refresh / first build. #}
{% if is_incremental() %}
{% set series_start = "date_trunc('week', " ~ as_of ~ " - (" ~ var('fair_recompute_weeks') ~ " * 7))::date" %}
{% else %}
{% set series_start = "date_trunc('week', '" ~ var('fair_history_start') ~ "'::date)::date" %}
{% endif %}
{% set tz = var('fair_tz') %}

with weeks as (
    select generate_series(
        {{ series_start }},
        (date_trunc('week', {{ as_of }}) - interval '7 days')::date,
        interval '7 days'
    )::date as week_start
),
daily as (
    select d.*, (date_trunc('week', d.local_day))::date as week_start,
           coalesce(c.is_cloudy, false) as is_cloudy
    from {{ ref('rec_fair_daily') }} d
    left join {{ ref('rec_fair_community_days') }} c
      on c.community_id = d.community_id and c.local_day = d.local_day
    where d.is_valid_day
),
weekly as (
    select k.device_id, k.community_id, k.week_start, k.pv_class, k.category, k.ref_group,
           k.is_quarantined, k.is_rankable,
           count(d.local_day)                                        as valid_days,
           sum(d.import_kwh)                                         as import_kwh,
           sum(d.night_import_kwh)                                   as night_import_kwh,
           sum(d.night_slots)                                        as night_slots,
           count(d.local_day) filter (where d.is_cloudy)             as cloudy_valid_days,
           sum(d.night_import_kwh) filter (where d.is_cloudy)
             / nullif(sum(d.import_kwh) filter (where d.is_cloudy), 0) as cloudy_night_share
    from {{ ref('rec_fair_device_class') }} k
    left join daily d on d.device_id = k.device_id and d.week_start = k.week_start
    group by k.device_id, k.community_id, k.week_start, k.pv_class, k.category, k.ref_group, k.is_quarantined, k.is_rankable
),
base as (
    select *,
           (valid_days >= {{ var('fair_valid_week_min_days') }})                     as is_valid_week,
           night_import_kwh / nullif(import_kwh, 0)                                  as night_share,
           night_import_kwh / nullif(night_slots / 4.0, 0)                           as night_kwh_h,
           {{ rec_season_start('week_start') }}                                       as season_start
    from weekly
),
level_raw as (
    select *,
      case when not is_valid_week then null
           when pv_class = 'no_pv' then night_share
           when cloudy_valid_days >= {{ var('fair_min_cloudy_days') }} then cloudy_night_share
      end as level_raw,
      case when not is_valid_week then 'no_valid_week'
           when pv_class = 'no_pv' then 'all_days'
           when cloudy_valid_days >= {{ var('fair_min_cloudy_days') }} then 'cloudy_days'
           else 'no_cloudy_days'
      end as level_source_raw
    from base
),
level_carry as (
    select *,
      case when level_raw is not null or not is_valid_week then level_raw
           else coalesce(lag(level_raw, 1) over d, lag(level_raw, 2) over d) end       as level_metric,
      case when level_raw is not null or not is_valid_week then level_source_raw
           when lag(level_raw, 1) over d is not null or lag(level_raw, 2) over d is not null then 'carry_forward'
           else level_source_raw end                                                   as level_source,
      case when level_raw is not null then 0
           when not is_valid_week then null
           when lag(level_raw, 1) over d is not null then 1
           when lag(level_raw, 2) over d is not null then 2
           else null end                                                               as level_carry_weeks
    from level_raw
    window d as (partition by device_id order by week_start)
),
valid as (
    select device_id, week_start, night_kwh_h, import_kwh, night_share from level_carry where is_valid_week
),
growth as (
    select v.device_id, v.week_start,
           case when k.k >= 2 * {{ var('fair_growth_min_n') }}
                then greatest({{ var('fair_growth_min_n') }}, least({{ var('fair_growth_max_n') }}, floor(k.k / 2.0)))::int end as growth_n
    from valid v
    cross join lateral (
        select count(*) as k from valid h
        where h.device_id = v.device_id and h.week_start <= v.week_start
          and h.week_start > v.week_start - {{ var('fair_growth_lookback_weeks') }} * 7
    ) k
),
growth_windows as (
    select g.device_id, g.week_start, g.growth_n,
           (select avg(night_kwh_h) from (
                select night_kwh_h from valid h
                where h.device_id = g.device_id and h.week_start <= g.week_start
                  and h.week_start > g.week_start - {{ var('fair_growth_lookback_weeks') }} * 7
                order by h.week_start desc limit g.growth_n) r)                       as growth_recent_kwh_h,
           (select avg(night_kwh_h) from (
                select night_kwh_h from valid h
                where h.device_id = g.device_id and h.week_start <= g.week_start
                  and h.week_start > g.week_start - {{ var('fair_growth_lookback_weeks') }} * 7
                order by h.week_start desc offset g.growth_n limit g.growth_n) p)      as growth_prev_kwh_h
    from growth g
    where g.growth_n is not null
),
rise as (
    select v.device_id, v.week_start,
           (select percentile_cont(0.5) within group (order by x.import_kwh) from (
                select import_kwh from valid h
                where h.device_id = v.device_id and h.week_start < v.week_start
                order by h.week_start desc limit {{ var('fair_import_median_weeks') }}) x) as import_median_8w,
           lag(v.night_share) over (partition by v.device_id order by v.week_start)       as night_share_prev
    from valid v
),
groups as (
    select community_id, week_start, ref_group, pv_class, count(*) as n
    from level_carry
    where is_valid_week and not is_quarantined and category in ('household', 'public_entity')
    group by community_id, week_start, ref_group, pv_class
),
merged_pv as (
    select community_id, week_start, pv_class
    from groups
    group by community_id, week_start, pv_class
    having min(n) < {{ var('fair_min_group_size') }}
),
effective as (
    select l.device_id, l.week_start,
           case when m.pv_class is not null and l.category in ('household', 'public_entity')
                then 'all/' || l.pv_class else l.ref_group end as ref_group_effective
    from level_carry l
    left join merged_pv m on m.community_id = l.community_id and m.week_start = l.week_start and m.pv_class = l.pv_class
),
effective_n as (
    select l.community_id, l.week_start, e.ref_group_effective, count(*) as ref_group_n
    from level_carry l join effective e using (device_id, week_start)
    where l.is_valid_week and not l.is_quarantined and l.category in ('household', 'public_entity')
    group by l.community_id, l.week_start, e.ref_group_effective
)
select
    md5(l.device_id || l.week_start::text)            as _id,
    l.device_id, l.community_id, l.week_start, l.season_start,
    l.valid_days, l.is_valid_week, l.import_kwh, l.night_import_kwh, l.night_slots,
    round(l.night_share::numeric, 6)::double precision as night_share,
    round(l.night_kwh_h::numeric, 6)::double precision as night_kwh_h,
    l.cloudy_valid_days,
    round(l.cloudy_night_share::numeric, 6)::double precision as cloudy_night_share,
    round(l.level_metric::numeric, 6)::double precision as level_metric,
    l.level_source, l.level_carry_weeks,
    g.growth_n, g.growth_recent_kwh_h, g.growth_prev_kwh_h,
    case when g.growth_prev_kwh_h >= {{ var('fair_growth_prev_floor_kwh_h') }}
         then round(((g.growth_recent_kwh_h - g.growth_prev_kwh_h) / g.growth_prev_kwh_h)::numeric, 6)::double precision end as growth_delta,
    r.import_median_8w,
    round((l.import_kwh / nullif(r.import_median_8w, 0) - 1)::numeric, 6)::double precision as import_rise_pct,
    r.night_share_prev,
    l.ref_group,
    e.ref_group_effective,
    coalesce(n.ref_group_n, 0)                        as ref_group_n,
    l.pv_class, l.category, l.is_quarantined, l.is_rankable
from level_carry l
left join growth_windows g using (device_id, week_start)
left join rise r using (device_id, week_start)
left join effective e using (device_id, week_start)
left join effective_n n on n.community_id = l.community_id and n.week_start = l.week_start and n.ref_group_effective = e.ref_group_effective
-- every CTE above runs over the whole history kept in rec_fair_device_class / rec_fair_daily
-- (growth windows, 8-week medians and carry-forward need it); only the output is windowed.
where l.week_start in (select week_start from weeks)
