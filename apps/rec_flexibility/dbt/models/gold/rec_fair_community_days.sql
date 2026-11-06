{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['rec_flexibility', 'fair']
  )
}}

-- Fair gamification: community M1 export per local day and the CAUSAL cloudy-day flag
-- (spec §4.4, 2026-10-09). A day is cloudy when the community export is at or below the
-- fair_cloudy_pct quantile of the fair_cloudy_lookback_days days ending on ITS WEEK'S
-- SUNDAY, so the flag for week W is computable on the following Monday and never looks
-- at later days. Only valid, non-quarantined devices contribute (a quarantined device
-- stops counting from quarantine_from); n_reporting records how many did.

with days as (
    select
        q.community_id,
        q.local_day,
        sum(q.export_kwh)  as community_export_kwh,
        count(*)           as n_reporting
    from {{ ref('rec_fair_daily') }} q
    join {{ ref('rec_fair_device_registry') }} r using (device_id)
    where q.is_valid_day
      and (r.quarantine_from is null or q.local_day < r.quarantine_from)
    group by q.community_id, q.local_day
),
with_week as (
    select *, (date_trunc('week', local_day))::date as week_start from days
),
thresholds as (
    select
        w.community_id,
        w.week_start,
        percentile_cont({{ var('fair_cloudy_pct') }}) within group (order by d.community_export_kwh) as cloudy_threshold_kwh
    from (select distinct community_id, week_start from with_week) w
    join days d
      on d.community_id = w.community_id
     and d.local_day between (w.week_start + 6) - ({{ var('fair_cloudy_lookback_days') }} - 1) and (w.week_start + 6)
    group by w.community_id, w.week_start
)
select
    md5(d.community_id || d.local_day::text)                 as _id,
    d.community_id,
    d.local_day,
    d.week_start,
    d.community_export_kwh,
    d.n_reporting,
    t.cloudy_threshold_kwh,
    (d.community_export_kwh <= t.cloudy_threshold_kwh)       as is_cloudy
from with_week d
join thresholds t using (community_id, week_start)
{% if is_incremental() %}
where d.local_day >= current_date - 42
{% endif %}
