{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=['rec_flexibility', 'fair']
  )
}}

-- Fair gamification anti-gaming flags (spec §4.4 / §8, design 2026-10-09), same shape as
-- rec_anti_gaming_flags. F1_IMPORT_RISE: the week's total import exceeds the device's own
-- 8-week median by more than fair_import_rise_pct while its night share fell — the
-- "daytime waste" pattern. Informational: no points effect; visible to managers.

select
    md5(device_id || week_start::text || 'F1_IMPORT_RISE')  as _id,
    device_id,
    community_id,
    week_start                                               as flag_date,
    'F1_IMPORT_RISE'                                         as flag_type,
    import_rise_pct                                          as metric_value,
    {{ var('fair_import_rise_pct') }}::double precision      as threshold_value,
    'warning'                                                as severity
from {{ ref('rec_fair_weekly_scores') }}
where flag_import_rise
