{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'ts_date',
      'device_id',
      'consumption_kwh',
      'co2_avoided_kg',
      'trees_equivalent'
    ]
  )
}}

-- CO2 avoided per device per day based on self-consumed renewable energy.
-- Source: rec_meters_15m (view over ds_dev_gold.meters_data_15m, kWh per bucket).
-- self_consumed_kwh is already kWh per 15-min bucket: summing buckets yields daily
-- kWh directly, with NO unit conversion.
-- Country factor from co2_factors seed (configured via var co2_country, default 'it').
-- Factors mirror webapp co2_settings.py constants.

with daily as (
    select
        device_id,
        ts::date                          as ts_date,
        sum(self_consumed_kwh)            as consumption_kwh
    from {{ ref('rec_meters_15m') }}

    {% if is_incremental() %}
    where ts >= date_trunc('day', now() - interval '2 days')
    {% endif %}

    group by device_id, ts::date
),

factor as (
    select kg_co2_per_kwh, trees_per_ton
    from {{ ref('co2_factors') }}
    where country = '{{ var("co2_country", "it") }}'
)

select
    md5(d.device_id || d.ts_date::text)                                         as _id,
    d.device_id,
    d.ts_date,
    round(d.consumption_kwh::numeric, 4)                                as consumption_kwh,
    round((d.consumption_kwh * f.kg_co2_per_kwh)::numeric, 3)          as co2_avoided_kg,
    round(
        (d.consumption_kwh * f.kg_co2_per_kwh / 1000 * f.trees_per_ton)::numeric,
        6
    )                                                                           as trees_equivalent
from daily d
cross join factor f
