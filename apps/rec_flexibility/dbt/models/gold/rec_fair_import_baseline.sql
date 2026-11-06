{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['rec_flexibility', 'fair']
  )
}}

-- Fair gamification: the device's "usual" import per quarter-of-day and weekday type,
-- used to judge kept commitments (spec §4.4, design 2026-10-09). For each completed week
-- W: median import per (slot, is_weekday) over the fair_baseline_days days BEFORE W's
-- Monday. Percent-based by construction: a small and a large home are each compared
-- with themselves.

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
)
select
    md5(m.device_id || w.week_start::text || m.slot::text || m.is_weekday::text) as _id,
    m.device_id,
    w.week_start,
    m.slot,
    m.is_weekday,
    percentile_cont(0.5) within group (order by m.import_kwh)                   as baseline_kwh
from weeks w
join {{ ref('rec_fair_meters_15m') }} m
  on m.local_day <  w.week_start
 and m.local_day >= w.week_start - {{ var('fair_baseline_days') }}
group by m.device_id, w.week_start, m.slot, m.is_weekday
