{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    merge_update_columns=[
      'community_id',
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
        community_id,
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

-- community_id: the measurement's community, carried from the upstream contract
-- unchanged. It is grouped on, not hashed: _id stays md5(device_id || ts), so rows
-- migrated in place keep their key. A device reported under two communities in one
-- slot gives two rows with one _id, which the unique test on _id fails.
select
    md5(device_id || ts::text) as _id,
    device_id,
    community_id,
    ts,
    sum(consumption_kwh)  as consumption_kwh,
    sum(production_kwh)   as production_kwh,
    sum(self_consumed_kwh) as self_consumed_kwh
from base
group by device_id, community_id, ts
