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

with hourly as (
    select
        date_trunc('hour', ts)        as ts,
        rec_id,
        substation_id,
        sum(total_consumption_kwh)    as total_consumption_kwh,
        sum(total_production_kwh)     as total_production_kwh,
        sum(self_consumption_kwh)     as self_consumption_kwh
    from {{ ref('rec_virtual_consumption_15m') }}

    {% if is_incremental() %}
    where ts >= (
        select coalesce(max(ts), '1900-01-01'::timestamp) - interval '2 hours'
        from {{ this }}
    )
    {% endif %}

    group by date_trunc('hour', ts), rec_id, substation_id
)

select
    ts,
    rec_id,
    substation_id,
    total_consumption_kwh,
    total_production_kwh,
    self_consumption_kwh,
    case
        when total_production_kwh > 0
        then self_consumption_kwh / total_production_kwh
        else 0
    end as self_consumption_ratio
from hourly
