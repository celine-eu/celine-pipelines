{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['rec_flexibility', 'fair']
  )
}}

-- Fair gamification: one row per (device, local day) with the data-quality flag
-- (spec §4.6: valid day = at least fair_valid_day_min_slots of 96 quarters) and the
-- day's import/export/night/midday aggregates. Incremental: the last 14 local days are
-- recomputed on every run (late meter rows), older days are frozen.

select
    md5(device_id || local_day::text)                              as _id,
    device_id,
    community_id,
    local_day,
    count(*)                                                       as n_slots,
    (count(*) >= {{ var('fair_valid_day_min_slots') }})            as is_valid_day,
    sum(import_kwh)                                                as import_kwh,
    sum(export_kwh)                                                as export_kwh,
    coalesce(sum(import_kwh) filter (where is_night), 0.0)         as night_import_kwh,
    count(*) filter (where is_night)                               as night_slots,
    coalesce(sum(import_kwh) filter (where is_midday), 0.0)        as midday_import_kwh
from {{ ref('rec_fair_meters_15m') }}
{% if is_incremental() %}
where local_day >= current_date - 14
{% endif %}
group by device_id, community_id, local_day
