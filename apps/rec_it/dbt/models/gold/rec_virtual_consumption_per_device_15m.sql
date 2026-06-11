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
        -- ×0.25 converts instantaneous kW snapshot to kWh for the 15-min slot.
        -- Assumes one reading per slot; event-triggered readings are approximated.
        m.consumption_kw * 0.25 as consumption_kwh
    from {{ source('rec_metering_gold', 'meters_data_15m') }} m
    join {{ ref('silver_rec_registry') }} r on m.device_id = r.sensor_id

    {% if is_incremental() %}
    where m.ts >= (
        select coalesce(max(ts), '1900-01-01'::timestamp) - interval '1 hour'
        from {{ this }}
    )
    {% endif %}
),

-- Reuse pre-computed community totals per (ts, rec_id, substation_id).
-- Avoids re-scanning meters_data_15m and ensures consistency with
-- community-level self-consumption figures.
community as (
    select
        ts,
        rec_id,
        substation_id,
        -- ×0.25 converts kW readings to kWh per 15-min slot (cancels in ratio but
        -- ensures available_kwh carries correct energy units for virtual allocation).
        total_consumption_kw * 0.25  as total_consumption_kwh,
        self_consumption_kw  * 0.25  as available_kwh
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
