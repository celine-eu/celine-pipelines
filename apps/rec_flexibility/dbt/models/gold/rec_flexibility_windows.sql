{#
  Run with --full-refresh when:
  - Stale rows appear (e.g. pre-device_id giant windows)
  - New columns are added to this model (incremental merge cannot add columns to existing tables)
  Example: dbt run --full-refresh --select rec_flexibility_windows
#}
{{
  config(
    materialized='incremental',
    unique_key='_id',
    incremental_strategy='merge',
    merge_update_columns=[
      'ts_date',
      'window_start',
      'window_end',
      'community_kwh',
      'estimated_kwh',
      'reward_points_estimated',
      'confidence',
      'flexibility_model'
    ]
  )
}}

-- Flexibility windows derived from the meter_forecasting pipeline output.
--
-- Source: ds_dev_gold.total_meters_forecast (community-level hourly surplus/deficit)
--         ds_dev_gold.meters_energy_forecast (per-device hourly consumption forecast)
--
-- Logic:
--   1. Find forecast hours where community net_exchange_kwh > THRESHOLD (solar surplus)
--   2. Skip sleeping hours (00:00–05:00)
--   3. Group consecutive surplus hours (gap > 1h = new window), partitioned by date
--   4. Cross-join with per-device consumption forecasts to compute personalized
--      estimated_kwh (how much community solar the device could absorb by shifting load)
--
-- Output: one row per (device_id, window) so the BFF fetcher can filter by device.

-- Community surplus threshold (kWh for a 1-hour period, community aggregate)
{% set EXPORT_THRESHOLD_KWH = 0.5 %}
-- Maximum hours per suggestion window — longer surplus periods are sliced into
-- chunks of this size so each suggestion is actionable (e.g. "run appliances 09:00–12:00")
{% set MAX_WINDOW_HOURS = 3 %}
-- Minimum surplus hours for a window to be shown (skip isolated 1h blips)
{% set MIN_WINDOW_HOURS = 2 %}

with latest_forecast as (
    select max(generated_at) as generated_at
    from {{ source('meters_gold', 'total_meters_forecast') }}
    where period = 'forecast'
),

surplus_forecast as (
    select
        f.timestamp::timestamp                  as ts,
        f.timestamp::timestamp::date            as ts_date,
        f.net_exchange_kwh
    from {{ source('meters_gold', 'total_meters_forecast') }} f
    cross join latest_forecast lf
    where f.period = 'forecast'
      and (f.generated_at = lf.generated_at or lf.generated_at is null)
      and f.net_exchange_kwh > {{ EXPORT_THRESHOLD_KWH }}
      and extract(hour from f.timestamp::timestamp) >= 5   -- skip sleeping hours 00:00–05:00

      {% if is_incremental() %}
      -- Refresh windows for today and tomorrow on every run
      and f.timestamp::timestamp::date >= current_date
      {% endif %}
),

-- Detect gaps: new window starts when previous surplus hour is > 1h ago.
-- Partition by ts_date so windows never cross midnight.
windowed as (
    select
        ts,
        ts_date,
        net_exchange_kwh,
        lag(ts) over (partition by ts_date order by ts) as prev_ts
    from surplus_forecast
),

-- Assign a window_group per continuous run of surplus hours within a day.
grouped as (
    select
        ts,
        ts_date,
        net_exchange_kwh,
        sum(
            case
                when prev_ts is null or ts - prev_ts > interval '1 hour' then 1
                else 0
            end
        ) over (partition by ts_date order by ts rows unbounded preceding) as window_group
    from windowed
),

-- Cap each window_group at MAX_WINDOW_HOURS by assigning a sub-group counter.
-- e.g. a 9-hour surplus run produces 3 × 3-hour sub-windows.
sub_grouped as (
    select
        ts,
        ts_date,
        net_exchange_kwh,
        window_group,
        floor(
            (row_number() over (partition by ts_date, window_group order by ts) - 1)
            / {{ MAX_WINDOW_HOURS }}
        )::int as sub_group
    from grouped
),

windows as (
    select
        ts_date,
        window_group,
        sub_group,
        min(ts)                         as window_start,
        max(ts) + interval '1 hour'     as window_end,
        sum(net_exchange_kwh)           as community_kwh
    from sub_grouped
    group by ts_date, window_group, sub_group
    having count(*) >= {{ MIN_WINDOW_HOURS }}
),

-- Per-device forecasted consumption during each surplus window.
-- For M1+M2 devices (total_consumption_kwh is set), use that.
-- For M1-only devices (no production meter), use grid_import_kwh.
-- This represents how much community solar energy the device could absorb
-- by shifting load into the window.
device_forecasts as (
    select
        device_id,
        ts,
        consumption_kwh
    from (
        select
            f.device_id,
            f.timestamp::timestamp                                    as ts,
            coalesce(f.total_consumption_kwh, f.grid_import_kwh, 0)  as consumption_kwh,
            row_number() over (
                partition by f.device_id, f.timestamp::timestamp
                order by f.generated_at desc
            ) as rn
        from {{ source('meters_gold', 'meters_energy_forecast') }} f
        where f.period = 'forecast'
          and f.data_missing = 0

          {% if is_incremental() %}
          and f.timestamp::timestamp::date >= current_date
          {% endif %}
    ) ranked
    where rn = 1
),

device_windows as (
    select
        df.device_id,
        w.ts_date,
        w.window_start,
        w.window_end,
        max(w.community_kwh)        as community_kwh,
        sum(df.consumption_kwh)     as estimated_kwh
    from windows w
    join device_forecasts df
        on df.ts >= w.window_start
       and df.ts < w.window_end
    group by
        df.device_id,
        w.ts_date,
        w.window_start,
        w.window_end
),

-- Sum forecasted consumption across all devices per window.
-- Used to proportionally cap each device's estimated_kwh so the community solar
-- budget (community_kwh) is never overflowed in aggregate estimates.
window_totals as (
    select
        window_start,
        window_end,
        sum(estimated_kwh) as total_device_kwh
    from device_windows
    group by window_start, window_end
)

select
    md5(dw.device_id || dw.ts_date::text || dw.window_start::text || dw.window_end::text)  as _id,
    dw.device_id,
    dw.ts_date,
    dw.window_start,
    dw.window_end,
    round(dw.community_kwh::numeric, 2)                                                     as community_kwh,
    -- Proportionally normalize per-device estimated_kwh so the sum across all devices
    -- in the same window never exceeds community_kwh (available solar surplus).
    -- When total device consumption already fits within the budget, no adjustment is made.
    round(
        case
            when wt.total_device_kwh > dw.community_kwh
            then dw.estimated_kwh / wt.total_device_kwh * dw.community_kwh
            else dw.estimated_kwh
        end
    ::numeric, 2)                                                                           as estimated_kwh,
    -- Log-scaled estimated points — matches rec_settlement_points (Task 9) under
    -- effort_multiplier = 1.0 on the estimate side. n_slots = duration (h) × 4.
    -- per_slot_kwh = capped_estimated_kwh / n_slots (uniform distribution across slots).
    -- estimated_points = n_slots × ln(1 + per_slot_kwh) × 10.
    -- This replaces the old linear round(estimated_kwh × 10) so adherence_ratio can
    -- compare apples to apples after rec_commitment_settlement switches to log+effort.
    round(
        (extract(epoch from (dw.window_end - dw.window_start)) / 900.0)
        * ln(
            1.0
            + (
                case
                    when wt.total_device_kwh > dw.community_kwh
                    then dw.estimated_kwh / wt.total_device_kwh * dw.community_kwh
                    else dw.estimated_kwh
                end
              ) / nullif(extract(epoch from (dw.window_end - dw.window_start)) / 900.0, 0)
          )
        * 10
    )::int                                                                                  as reward_points_estimated,
    -- TODO [CELINE-FLEX-CONF-CAL]: derive per-category confidence from historical
    -- calibration — forecast net_exchange_kwh vs. actual metered surplus per window,
    -- averaged by device category (households / public_buildings / unclassified).
    -- Data sources: gamification CSV (~7 months of history) + ds_dev_silver.meters_data
    -- (live 2.5 months). 0.75 is a placeholder until the calibration seed lands.
    0.75::numeric                                           as confidence,
    -- flexibility_model tags the mechanism that generated this window.
    -- Set via dbt var so future models (e.g. local_flex_market) use a separate run
    -- with --vars '{"flexibility_model": "local_flex_market"}'.
    -- rec_commitment_settlement carries suggestion_type from the actual commitment record,
    -- providing the definitive per-commitment model link.
    '{{ var("flexibility_model", "solar_overproduction") }}'::text  as flexibility_model
from device_windows dw
join window_totals wt using (window_start, window_end)
where dw.estimated_kwh > 0
