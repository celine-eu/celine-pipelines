{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['rec_flexibility', 'fair']
  )
}}

-- Fair gamification: the class of every device for every completed ISO week
-- (spec §4.2/§4.6, design 2026-10-09). Recomputed for the trailing
-- fair_recompute_weeks weeks on each run, so a late override or late data
-- re-classifies recent weeks; older weeks are frozen.
--
-- pv_class is DATA-DRIVEN (never the registry role): over the fair_pv_lookback_days
-- days ending on the week's Sunday, daily M1 export above fair_pv_export_kwh_day on at
-- least (1 − fair_pv_export_quantile) of valid days → 'pv' (pv_source 'export'); else a
-- meter without export register (CF1/CF3) whose midday import on clear days is below
-- fair_hidden_pv_midday_ratio × that on cloudy days → 'pv' ('hidden'); else 'no_pv'.
-- A pv_class_override from the overrides seed wins ('override').
-- is_silent: last reading older than fair_silent_days on the week's Sunday.
-- is_quarantined: quarantine_from on or before the week's Sunday.
-- is_rankable: in app, category household/public_entity, not quarantined, not silent.

{% set as_of = ("'" ~ var('fair_as_of') ~ "'::date") if var('fair_as_of', '') else 'current_date' %}
{# Weeks (re)computed this run: the trailing fair_recompute_weeks on an incremental run,
   every completed week since fair_history_start on a full refresh / first build. #}
{% if is_incremental() %}
{% set series_start = "date_trunc('week', " ~ as_of ~ " - (" ~ var('fair_recompute_weeks') ~ " * 7))::date" %}
{% else %}
{% set series_start = "date_trunc('week', '" ~ var('fair_history_start') ~ "'::date)::date" %}
{% endif %}

with weeks as (
    select generate_series(
        {{ series_start }},
        (date_trunc('week', {{ as_of }}) - interval '7 days')::date,
        interval '7 days'
    )::date as week_start
),
dev_week as (
    select r.device_id, r.community_id, r.is_in_app, r.category, r.has_export_register,
           r.pv_class_override, r.quarantine_from, r.battery_declared,
           w.week_start, (w.week_start + 6) as week_end
    from {{ ref('rec_fair_device_registry') }} r
    cross join weeks w
),
export_window as (
    select dw.device_id, dw.week_start,
           percentile_cont({{ var('fair_pv_export_quantile') }}) within group (order by d.export_kwh) as export_kwh_day_median
    from dev_week dw
    join {{ ref('rec_fair_daily') }} d
      on d.device_id = dw.device_id
     and d.is_valid_day
     and d.local_day <= dw.week_end
     and d.local_day > dw.week_end - {{ var('fair_pv_lookback_days') }}
    group by dw.device_id, dw.week_start
),
last_reading as (
    select dw.device_id, dw.week_start, max(d.local_day) as last_reading_at
    from dev_week dw
    join {{ ref('rec_fair_daily') }} d
      on d.device_id = dw.device_id and d.local_day <= dw.week_end
    group by dw.device_id, dw.week_start
),
midday as (
    select dw.device_id, dw.week_start,
           avg(d.midday_import_kwh) filter (where not c.is_cloudy)
             / nullif(avg(d.midday_import_kwh) filter (where c.is_cloudy), 0) as midday_clear_cloudy_ratio
    from dev_week dw
    join {{ ref('rec_fair_daily') }} d
      on d.device_id = dw.device_id
     and d.is_valid_day
     and d.local_day <= dw.week_end
     and d.local_day > dw.week_end - {{ var('fair_pv_lookback_days') }}
    join {{ ref('rec_fair_community_days') }} c
      on c.community_id = d.community_id and c.local_day = d.local_day
    group by dw.device_id, dw.week_start
),
classified as (
    select
        dw.*,
        e.export_kwh_day_median,
        m.midday_clear_cloudy_ratio,
        l.last_reading_at,
        case
            when dw.pv_class_override is not null then dw.pv_class_override
            when e.export_kwh_day_median > {{ var('fair_pv_export_kwh_day') }} then 'pv'
            when not dw.has_export_register
                 and m.midday_clear_cloudy_ratio < {{ var('fair_hidden_pv_midday_ratio') }} then 'pv'
            else 'no_pv'
        end as pv_class,
        case
            when dw.pv_class_override is not null then 'override'
            when e.export_kwh_day_median > {{ var('fair_pv_export_kwh_day') }} then 'export'
            when not dw.has_export_register
                 and m.midday_clear_cloudy_ratio < {{ var('fair_hidden_pv_midday_ratio') }} then 'hidden'
            else 'none'
        end as pv_source,
        (dw.quarantine_from is not null and dw.quarantine_from <= dw.week_end) as is_quarantined,
        (l.last_reading_at is null
            or dw.week_end - l.last_reading_at > {{ var('fair_silent_days') }}) as is_silent
    from dev_week dw
    left join export_window e using (device_id, week_start)
    left join midday m using (device_id, week_start)
    left join last_reading l using (device_id, week_start)
)
select
    md5(device_id || week_start::text)                       as _id,
    device_id,
    community_id,
    week_start,
    pv_class,
    pv_source,
    export_kwh_day_median,
    midday_clear_cloudy_ratio,
    category,
    is_in_app,
    battery_declared,
    is_quarantined,
    last_reading_at,
    is_silent,
    (is_in_app
        and category in ('household', 'public_entity')
        and not is_quarantined
        and not is_silent)                                   as is_rankable,
    category || '/' || pv_class                              as ref_group
from classified
