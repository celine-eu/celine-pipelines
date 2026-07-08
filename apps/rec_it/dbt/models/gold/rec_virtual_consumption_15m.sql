{{
  config(
    materialized='incremental',
    unique_key=['ts', 'rec_id', 'substation_id'],
    incremental_strategy='merge',
    merge_update_columns=[
      'total_consumption_kwh',
      'total_production_kwh',
      'self_consumption_kwh',
      'self_consumption_ratio'
    ]
  )
}}

-- Design notes
-- ============
-- Unregistered devices: meters absent from silver_rec_registry are excluded.
--   They are not REC participants; including them would dilute self-consumption
--   figures with energy flows unrelated to the community.
--
-- Production attribution: only role='prosumer' devices contribute production_kwh
--   to the community pool. role='consumer' devices are consumption-only.

with metering as (
    select
        m.ts,
        r.rec_id,
        r.substation_id,
        m.consumption_kwh,
        case
            when r.role = 'prosumer' then coalesce(m.production_kwh, 0)
            else 0
        end as production_kwh
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
        sum(consumption_kwh) as total_consumption_kwh,
        sum(production_kwh)  as total_production_kwh
    from metering
    group by ts, rec_id, substation_id
)

select
    ts,
    rec_id,
    substation_id,
    total_consumption_kwh,
    total_production_kwh,
    least(total_consumption_kwh, total_production_kwh) as self_consumption_kwh,
    case
        when total_production_kwh > 0
        then least(total_consumption_kwh, total_production_kwh) / total_production_kwh
        else 0
    end as self_consumption_ratio
from community
