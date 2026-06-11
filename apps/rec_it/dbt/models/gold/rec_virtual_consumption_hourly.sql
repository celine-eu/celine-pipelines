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

with hourly as (
    select
        date_trunc('hour', ts)        as ts,
        rec_id,
        substation_id,
        sum(total_consumption_kw)     as total_consumption_kw,
        sum(total_production_kw)      as total_production_kw,
        sum(self_consumption_kw)      as self_consumption_kw
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
    total_consumption_kw,
    total_production_kw,
    self_consumption_kw,
    case
        when total_production_kw > 0
        then self_consumption_kw / total_production_kw
        else 0
    end as self_consumption_ratio
from hourly
