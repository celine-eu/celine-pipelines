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

with device as (
    select
        m.device_id,
        r.rec_id,
        r.substation_id,
        m.ts,
        m.consumption_kwh
    from {{ source('rec_metering_gold', 'meters_data_15m') }} m
    join {{ ref('silver_rec_registry') }} r on m.device_id = r.sensor_id

    {% if is_incremental() %}
    where m.ts >= (
        select coalesce(max(ts), '1900-01-01'::timestamp) - interval '1 hour'
        from {{ this }}
    )
    {% endif %}
),

community as (
    select
        ts,
        rec_id,
        substation_id,
        total_consumption_kwh,
        self_consumption_kwh as available_kwh
    from {{ ref('rec_virtual_consumption_15m') }}

    {% if is_incremental() %}
    where ts >= (
        select coalesce(max(ts), '1900-01-01'::timestamp) - interval '1 hour'
        from {{ this }}
    )
    {% endif %}
)

select
    md5(d.device_id || d.ts::text || d.rec_id || d.substation_id)  as _id,
    d.ts,
    d.device_id,
    d.rec_id,
    d.substation_id,
    d.consumption_kwh,
    case
        when c.total_consumption_kwh > 0
        then d.consumption_kwh / c.total_consumption_kwh
        else 0
    end                             as ratio,
    case
        when c.total_consumption_kwh > 0
        then (d.consumption_kwh / c.total_consumption_kwh) * c.available_kwh
        else 0
    end                             as virtual_consumption_kwh
from device d
join community c
    on  d.ts           = c.ts
    and d.rec_id       = c.rec_id
    and d.substation_id = c.substation_id
