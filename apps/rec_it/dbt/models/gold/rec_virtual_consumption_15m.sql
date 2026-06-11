{{
  config(
    materialized='incremental',
    unique_key=['ts', 'rec_id', 'substation_id'],
    incremental_strategy='merge',
    merge_update_columns=[
      'total_consumption_kw',
      'total_production_kw',
      'self_consumption_kw',
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
-- Production attribution: only role='prosumer' devices contribute production_kw
--   to the community pool. role='consumer' devices are consumption-only.

with metering as (
    select
        m.ts,
        r.rec_id,
        r.substation_id,
        m.consumption_kw,
        case
            when r.role = 'prosumer' then coalesce(m.production_kw, 0)
            else 0
        end as production_kw
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
        sum(consumption_kw) as total_consumption_kw,
        sum(production_kw)  as total_production_kw
    from metering
    group by ts, rec_id, substation_id
)

select
    ts,
    rec_id,
    substation_id,
    total_consumption_kw,
    total_production_kw,
    least(total_consumption_kw, total_production_kw) as self_consumption_kw,
    case
        when total_production_kw > 0
        then least(total_consumption_kw, total_production_kw) / total_production_kw
        else 0
    end as self_consumption_ratio
from community
