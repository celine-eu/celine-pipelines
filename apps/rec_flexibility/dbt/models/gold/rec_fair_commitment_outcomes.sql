{{
  config(
    materialized='incremental',
    unique_key='commitment_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['rec_flexibility', 'fair']
  )
}}

-- Fair gamification: one outcome per commitment (spec §4.4 Azioni, design 2026-10-09).
--
-- Matching: the commitment period is matched to the COMMUNITY window it overlaps most
-- (time overlap, never bound equality — windows are rebuilt daily and their bounds move).
-- One COUNTED outcome per (device, window): the earliest committed one; later ones are
-- 'duplicate_window' (0 points). Only commitments of completed weeks are judged.
-- Kept: import over the commitment period ≥ fair_kept_ratio × the device's own slot
-- baseline (rec_fair_import_baseline, floored at fair_baseline_floor_kwh), compared at 6
-- decimals so exact boundaries (1.10×) count as kept. No overlapping window → 'no_window'
-- (not evaluable, never "missed"); no meter rows or no baseline for the week → 'no_data'.
-- Surplus extra: hours inside the period where the device's substation (fallback:
-- community) is in true surplus AND its hourly import ≥ ratio × hourly baseline.
-- Windows are naive local wall-clock timestamps (verified against measured surplus
-- hours on 2026-10-09); they are cast with `at time zone fair_tz`.
-- Source: rec_commitment_settlement (durable), not the 90-day mirror.

{% set as_of = ("'" ~ var('fair_as_of') ~ "'::date") if var('fair_as_of', '') else 'current_date' %}

with c as (
    select commitment_id, device_id, community_id, period_start, period_end, committed_at,
           (date_trunc('week', period_start at time zone '{{ var("fair_tz") }}'))::date as week_start
    from {{ ref('rec_commitment_settlement') }}
    where status in ('committed', 'settled')
      -- outcomes land once a week: only commitments of COMPLETED weeks are judged, so the
      -- week's import baseline exists and a verdict is never revised after it is settled
      and (date_trunc('week', period_start at time zone '{{ var("fair_tz") }}'))::date + 7 <= {{ as_of }}
    {% if is_incremental() %}
      and period_start >= now() - interval '35 days'
    {% endif %}
),
w as (
    select community_id,
           window_start at time zone '{{ var("fair_tz") }}' as window_start,
           window_end   at time zone '{{ var("fair_tz") }}' as window_end
    from {{ ref('rec_flexibility_windows_community') }}
),
cand as (
    select c.*, w.window_start, w.window_end,
           least(w.window_end, c.period_end) - greatest(w.window_start, c.period_start) as overlap
    from c
    left join w
      on w.community_id = c.community_id
     and w.window_start < c.period_end
     and w.window_end   > c.period_start
),
best_window as (
    select distinct on (commitment_id) *
    from cand
    order by commitment_id, overlap desc nulls last
),
one_per_window as (
    -- one COUNTED commitment per (device, community window): the earliest committed; the
    -- others keep a row ('duplicate_window', 0 points) so the flexibility-api can settle them
    select *,
           (window_start is not null
              and row_number() over (partition by device_id, window_start order by committed_at, commitment_id) = 1) as counted
    from best_window
),
slots as (
    select o.commitment_id, o.community_id, m.substation_id,
           date_trunc('hour', m.ts) as hour_start,
           m.import_kwh,
           b.baseline_kwh
    from one_per_window o
    join {{ ref('rec_fair_meters_15m') }} m
      on m.device_id = o.device_id
     and m.ts >= o.period_start
     and m.ts <  o.period_end
    left join {{ ref('rec_fair_import_baseline') }} b
      on b.device_id = m.device_id
     and b.week_start = o.week_start
     and b.slot = m.slot
     and b.is_weekday = m.is_weekday
    where o.counted
),
hourly as (
    select s.commitment_id, s.hour_start,
           sum(s.import_kwh)                         as imp,
           sum(coalesce(s.baseline_kwh, 0.0))        as base,
           count(s.baseline_kwh)                     as base_rows,
           coalesce(bool_or(sub.is_surplus), bool_or(com.is_surplus), false) as is_surplus
    from slots s
    left join {{ ref('rec_fair_surplus_hours') }} sub
      on sub.scope_type = 'substation' and sub.community_id = s.community_id
     and sub.scope_id = s.substation_id and sub.hour_start = s.hour_start
    left join {{ ref('rec_fair_surplus_hours') }} com
      on com.scope_type = 'community' and com.community_id = s.community_id
     and com.hour_start = s.hour_start
    group by s.commitment_id, s.hour_start
),
energy as (
    select commitment_id,
           sum(imp)                                                   as actual_import_kwh,
           -- no baseline row at all → null (not evaluable); the floor only lifts a near-zero one
           case when sum(base_rows) = 0 then null
                else greatest({{ var('fair_baseline_floor_kwh') }}, sum(base)) end as baseline_import_kwh,
           count(*) filter (
               where is_surplus
                 and base_rows > 0
                 and round(imp::numeric, 6) >= round(({{ var('fair_kept_ratio') }} * base)::numeric, 6)
           )                                                          as surplus_hours_kept
    from hourly
    group by commitment_id
),
judged as (
    select o.commitment_id, o.device_id, o.community_id, o.week_start, o.period_start, o.period_end,
           o.counted                                                as matched,
           (o.window_start is not null)                             as has_window,
           o.window_start, o.window_end,
           e.actual_import_kwh,
           e.baseline_import_kwh,
           (o.counted
              and e.actual_import_kwh is not null
              and e.baseline_import_kwh is not null
              and round(e.actual_import_kwh::numeric, 6)
                  >= round(({{ var('fair_kept_ratio') }} * e.baseline_import_kwh)::numeric, 6)) as is_kept,
           coalesce(e.surplus_hours_kept, 0) as surplus_hours_kept
    from one_per_window o
    left join energy e using (commitment_id)
)
select
    commitment_id, device_id, community_id, week_start, period_start, period_end,
    matched, window_start, window_end,
    actual_import_kwh, baseline_import_kwh,
    actual_import_kwh / nullif(baseline_import_kwh, 0)              as ratio,
    is_kept,
    case when is_kept then {{ var('fair_points_per_kept') }} else 0 end as points,
    case when matched then surplus_hours_kept else 0 end            as surplus_hours_kept,
    case when matched then surplus_hours_kept else 0 end * {{ var('fair_surplus_points_per_hour') }} as surplus_points,
    case
        when not has_window                                    then 'no_window'
        when not matched                                       then 'duplicate_window'
        when actual_import_kwh is null or baseline_import_kwh is null then 'no_data'
        when is_kept                                           then 'kept'
        else 'not_kept'
    end                                                             as reason
from judged
