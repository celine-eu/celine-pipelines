{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['rec_flexibility', 'fair']
  )
}}

-- Fair gamification: "true surplus" hours (spec §4.4 Azioni, design 2026-10-09).
-- An hour is in surplus for a scope when the M1 export of its non-quarantined PODs
-- exceeds their import. Scope = the primary substation (registry substation_id) when
-- at least fair_substation_min_devices PODs reported in that hour; the whole community
-- always gets its own row and is the fallback for devices whose substation has none.
-- Computed directly from the meter readings (never from the forecast text timestamps).

with f as (
    select m.community_id, m.substation_id, m.device_id,
           date_trunc('hour', m.ts) as hour_start,
           m.import_kwh, m.export_kwh
    from {{ ref('rec_fair_meters_15m') }} m
    join {{ ref('rec_fair_device_registry') }} r using (device_id)
    where r.quarantine_from is null or m.local_day < r.quarantine_from
    {% if is_incremental() %}
      and m.ts >= now() - interval '35 days'
    {% endif %}
),
hourly as (
    select community_id, substation_id, hour_start,
           sum(export_kwh)            as export_kwh,
           sum(import_kwh)            as import_kwh,
           count(distinct device_id)  as n_devices
    from f
    group by community_id, substation_id, hour_start
),
scoped as (
    select community_id, 'substation' as scope_type, substation_id as scope_id, hour_start, export_kwh, import_kwh, n_devices
    from hourly
    where substation_id is not null
      and n_devices >= {{ var('fair_substation_min_devices') }}
    union all
    select community_id, 'community' as scope_type, community_id as scope_id, hour_start,
           sum(export_kwh), sum(import_kwh), sum(n_devices)
    from hourly
    group by community_id, hour_start
)
select
    md5(scope_type || scope_id || hour_start::text) as _id,
    community_id,
    scope_type,
    scope_id,
    hour_start,
    export_kwh,
    import_kwh,
    n_devices,
    (export_kwh > import_kwh)                        as is_surplus
from scoped
