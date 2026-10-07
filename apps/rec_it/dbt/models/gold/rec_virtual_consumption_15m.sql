{{
  config(
    materialized='incremental',
    unique_key=['ts', 'community_id', 'substation_id'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
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
-- Membership: a reading counts only when its (device_id, community_id) pair is a
--   row of rec_device_membership (rec_registry). A device the registry does not list
--   under the reading's community is excluded: it is not a participant there, and
--   including it would dilute the figures with unrelated energy flows. A device listed
--   under two communities contributes each reading only to the community the reading
--   carries; membership is unique per pair, so no reading is counted twice.
--
-- Production attribution: only role='prosumer' devices contribute production_kwh
--   to the community pool. role='consumer' devices are consumption-only.

with metering as (
    select
        m.ts,
        m.community_id,
        r.substation_id,
        m.consumption_kwh,
        case
            when r.role = 'prosumer' then coalesce(m.production_kwh, 0)
            else 0
        end as production_kwh
    from {{ source('rec_metering_gold', 'meters_data_15m') }} m
    join {{ source('rec_registry_gold', 'rec_device_membership') }} r
      on  r.device_id    = m.device_id
      and r.community_id = m.community_id

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
        community_id,
        substation_id,
        sum(consumption_kwh) as total_consumption_kwh,
        sum(production_kwh)  as total_production_kwh
    from metering
    group by ts, community_id, substation_id
)

select
    ts,
    community_id,
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
