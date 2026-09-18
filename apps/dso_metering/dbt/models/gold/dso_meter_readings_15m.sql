-- The grid operator's quarter-hour readings, as this repository publishes them.
--
-- Deliberately a projection and not an aggregation. `rec_metering` sums its
-- source per (device_id, ts) to collapse duplicates; here the grain is the
-- operator's own statement about its own meter, so a second row for the same
-- interval means the export disagrees with itself and must be seen, not summed.
-- The `unique` test on `_id` is what sees it.
--
-- Nothing is derived, converted or clipped: the two measures are kWh per
-- interval in the source and kWh per interval here.
{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'consumption_kwh',
      'production_kwh',
      'reading_quality'
    ]
  )
}}

select
    md5(pod_code || ts::text) as _id,
    pod_code,
    ts,
    consumption_kwh,
    production_kwh,
    reading_quality
from {{ source('dso_metering_silver', 'silver_dso_meter_readings') }}

{% if is_incremental() %}
-- One interval of overlap, so a late correction to the last slot is merged
-- rather than left at its first value.
where ts >= (
    select coalesce(max(ts), '1900-01-01'::timestamptz) - interval '15 minutes'
    from {{ this }}
)
{% endif %}
