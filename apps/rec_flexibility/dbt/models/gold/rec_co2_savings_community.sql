{{
  config(
    materialized='incremental',
    unique_key='ts_date',
    incremental_strategy='merge',
    merge_update_columns=[
      'consumption_kwh',
      'co2_avoided_kg',
      'trees_equivalent',
      'participating_devices'
    ]
  )
}}

-- Community-level CO2 avoided attributable to the flexibility programme.
-- Only 15-min intervals that fall inside a flexibility window (window_start IS NOT NULL
-- in rec_settlement_15m) are included — this scopes the metric to energy shifts
-- that the flexibility programme actually induced, not ambient self-consumption.
-- Device-level daily CO2 (all self-consumption, all day) lives in rec_co2_savings.

with window_intervals as (
    select
        device_id,
        ts
    from {{ ref('rec_settlement_15m') }}
    where window_start is not null

    {% if is_incremental() %}
    and ts >= date_trunc('day', now() - interval '2 days')
    {% endif %}
),

daily as (
    -- self_consumed_kw is avg kW over the 15-min bucket → ×0.25 for kWh per slot.
    -- Previous revision summed kW directly (4× overcount). See Task 0 Step 0.5.
    select
        m.ts::date                          as ts_date,
        sum(m.self_consumed_kwh) * 0.25     as consumption_kwh,
        count(distinct m.device_id)         as participating_devices
    from {{ ref('rec_meters_15m') }} m
    join window_intervals w
        on  m.device_id = w.device_id
        and m.ts        = w.ts

    {% if is_incremental() %}
    where m.ts >= date_trunc('day', now() - interval '2 days')
    {% endif %}

    group by m.ts::date
),

factor as (
    select kg_co2_per_kwh, trees_per_ton
    from {{ ref('co2_factors') }}
    where country = '{{ var("co2_country", "it") }}'
)

select
    d.ts_date,
    round(d.consumption_kwh::numeric, 4)                                         as consumption_kwh,
    round((d.consumption_kwh * f.kg_co2_per_kwh)::numeric, 3)                   as co2_avoided_kg,
    round(
        (d.consumption_kwh * f.kg_co2_per_kwh / 1000 * f.trees_per_ton)::numeric,
        6
    )                                                                             as trees_equivalent,
    d.participating_devices
from daily d
cross join factor f
