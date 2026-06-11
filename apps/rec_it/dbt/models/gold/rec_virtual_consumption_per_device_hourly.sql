{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'ts',
      'device_id',
      'rec_id',
      'substation_id',
      'consumption_kwh',
      'virtual_consumption_kwh',
      'ratio'
    ]
  )
}}

with hourly as (
    select
        date_trunc('hour', ts)          as ts,
        device_id,
        rec_id,
        substation_id,
        sum(consumption_kwh)            as consumption_kwh,
        sum(virtual_consumption_kwh)    as virtual_consumption_kwh
    from {{ ref('rec_virtual_consumption_per_device_15m') }}

    {% if is_incremental() %}
    where ts >= (
        select coalesce(max(ts), '1900-01-01'::timestamp) - interval '2 hours'
        from {{ this }}
    )
    {% endif %}

    group by date_trunc('hour', ts), device_id, rec_id, substation_id
)

select
    md5(device_id || ts::text || rec_id || substation_id)  as _id,
    ts,
    device_id,
    rec_id,
    substation_id,
    consumption_kwh,
    virtual_consumption_kwh,
    case
        when sum(consumption_kwh) over (partition by ts, rec_id, substation_id) > 0
        then consumption_kwh / sum(consumption_kwh) over (partition by ts, rec_id, substation_id)
        else 0
    end                         as ratio
from hourly
