{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'ts',
      'consumption_kw',
      'production_kw',
      'self_consumed_kw'
    ]
  )
}}

with base as (
    select
        device_id,
        ts,
        consumption_kw,
        production_kw,
        self_consumed_kw
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
    sum(consumption_kw)  as consumption_kw,
    sum(production_kw)   as production_kw,
    sum(self_consumed_kw) as self_consumed_kw
from base
group by device_id, ts
