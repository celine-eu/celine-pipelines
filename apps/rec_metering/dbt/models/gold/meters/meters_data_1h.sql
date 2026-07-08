{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'ts',
      'consumption_kwh',
      'production_kwh'
    ]
  )
}}

with base as (
    select
        device_id,
        ts,
        consumption_kwh,
        production_kwh
    from {{ ref('meters_data_15m') }}
),

aggregated as (
    select
        md5(device_id || date_trunc('hour', ts)::text) as _id,  -- unique per device per hour
        device_id,
        date_trunc('hour', ts) as ts,
        sum(consumption_kwh) as consumption_kwh,
        sum(production_kwh) as production_kwh
    from base
    group by device_id, date_trunc('hour', ts)
),

last_extracted as (
  {% if is_incremental() %}
    select coalesce(max(ts), '1900-01-01'::timestamp) - interval '1 hour' as last_ts
    from {{ this }}
  {% else %}
    select '1900-01-01'::timestamp as last_ts
  {% endif %}
)

select *
from aggregated

{% if is_incremental() %}
where ts >= (select last_ts from last_extracted)
{% endif %}
