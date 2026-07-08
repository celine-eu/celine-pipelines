{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'ts',
      'consumption_kwh',
      'production_kwh',
      'self_consumed_kwh'
    ]
  )
}}

with base as (
    select
        device_id,
        ts,
        consumption_kwh,
        production_kwh,
        self_consumed_kwh
    from {{ source('metering_silver', 'meters_data_normalized') }}

    {% if is_incremental() %}
    where ts >= (
        select coalesce(max(ts), '1900-01-01'::timestamp) - interval '1 hour'
        from {{ this }}
    )
    {% endif %}
)

select
    md5(device_id || ts::text) as _id,
    device_id,
    ts,
    sum(consumption_kwh)  as consumption_kwh,
    sum(production_kwh)   as production_kwh,
    sum(self_consumed_kwh) as self_consumed_kwh
from base
group by device_id, ts
