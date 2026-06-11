{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'ts',
      'device_id',
      'consumption_kwh',
      'window_start',
      'window_end',
      'ts_date',
      'window_estimated_kwh',
    ]
  )
}}

-- Hourly rollup of rec_settlement_15m.
-- Consumed by flexibility-api settlement: query by device_id + window timestamp range,
-- sum consumption_kwh to get actual kWh consumed during a committed flexibility window.
-- window_start/window_end are non-null when the hour falls inside a flexibility window.

select
    md5(device_id || date_trunc('hour', ts)::text) as _id,
    date_trunc('hour', ts)                          as ts,
    device_id,
    sum(consumption_kwh)                            as consumption_kwh,
    -- Carry window context: non-null when any 15m interval in this hour is inside a window.
    -- Window generation constraints ensure at most one window per device per hour.
    max(window_start)                               as window_start,
    max(window_end)                                 as window_end,
    max(ts_date)                                    as ts_date,
    max(window_estimated_kwh)                       as window_estimated_kwh
from {{ ref('rec_settlement_15m') }}

{% if is_incremental() %}
where ts >= date_trunc('day', now() - interval '2 days')
{% endif %}

group by 1, 2, 3
