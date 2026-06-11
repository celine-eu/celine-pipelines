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

with base as (
    select
        device_id,
        ts
    from {{ ref('meters_data_15m') }}
    where ts >= date_trunc('day', current_timestamp) - interval '7 days'
    {% if is_incremental() %}
    and ts >= (
        select coalesce(max(ts), '1900-01-01'::timestamp) - interval '1 hour'
        from {{ this }}
    )
    {% endif %}
),

-- generate all expected 15-min timestamps per device
expected_ts as (
    select
        device_id,
        generate_series(min(ts), max(ts), interval '15 minutes') as expected_ts
    from base
    group by device_id
),

-- detect missing intervals
missing_intervals as (
    select
        md5(e.device_id || e.expected_ts) as event_id,
        e.device_id,
        e.expected_ts as ts,
        extract(hour from e.expected_ts AT TIME ZONE 'Europe/Rome') as hour,
        current_timestamp as created_at
    from expected_ts e
    left join base b
      on b.device_id = e.device_id and b.ts = e.expected_ts
    where b.ts is null
)

select *
from missing_intervals
