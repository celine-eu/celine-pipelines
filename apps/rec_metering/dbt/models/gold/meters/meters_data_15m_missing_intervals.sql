{{
  config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'device_id',
      'ts',
      'created_at'
    ]
  )
}}

with detection_window as (
    select
        date_trunc('hour', current_timestamp) + interval '15 minutes' * floor(extract(minute from current_timestamp) / 15) - interval '7 days' as detection_window_start,
        date_trunc('hour', current_timestamp) + interval '15 minutes' * floor(extract(minute from current_timestamp) / 15)                     as detection_window_end
),

known_devices as (
    select distinct device_id
    from {{ ref('meters_data_15m') }}
),

expected_ts as (
    select
        d.device_id,
        g.expected_ts
    from known_devices d
    cross join detection_window w
    cross join lateral generate_series(w.detection_window_start, w.detection_window_end, interval '15 minutes') as g(expected_ts)
),

actual as (
    select
        device_id,
        ts
    from {{ ref('meters_data_15m') }}
    cross join detection_window w
    where ts >= w.detection_window_start
      and ts <= w.detection_window_end
),

missing_intervals as (
    select
        md5(e.device_id || e.expected_ts::text) as event_id,
        e.device_id,
        e.expected_ts as ts,
        extract(hour from e.expected_ts AT TIME ZONE 'Europe/Rome') as hour,
        current_timestamp as created_at
    from expected_ts e
    left join actual a
      on a.device_id = e.device_id and a.ts = e.expected_ts
    where a.ts is null
)

select *
from missing_intervals

{% if is_incremental() %}
where ts >= (
    select coalesce(max(ts), '1900-01-01'::timestamp) - interval '1 hour'
    from {{ this }}
)
{% endif %}
