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
    full_refresh=false,
    merge_update_columns=[
      'ts_date',
      'window_start',
      'window_end',
      'community_kwh',
      'estimated_kwh',
      'reward_points_estimated',
      'confidence',
      'flexibility_model'
    ],
    pre_hook="{% if is_incremental() %}delete from {{ this }} where ts_date >= current_date{% endif %}"
  )
}}

-- full_refresh=false: accumulate-only, same rationale as rec_flexibility_windows_community
-- (derives from the latest ~48h forecast generation; history is unrecoverable from source).

-- Per-device flexibility window enrichment — v3 "Fair and Square" personal potential.
--
-- Detection lives in rec_flexibility_windows_community (EVENT layer). This model
-- (ENRICHMENT layer) promises each device what it could plausibly earn:
--   estimated_kwh = expected (settlement baseline) + potential (upward spread),
-- with the M1-only potential capped by export_median + import_spread (you cannot
-- dip export below zero). Devices without shift_potential baselines (<14 days of
-- history) fall back to the legacy forecast-based estimate. A device must have at
-- least one usable forecast row inside the window (liveness gate) — an offline
-- meter must not carry a promise.
-- Spec: demo3/docs/superpowers/specs/2026-07-06-flexibility-window-points-v3-design.md

with windows as (
    select
        ts_date,
        window_start,
        window_end,
        community_kwh,
        confidence
    from {{ ref('rec_flexibility_windows_community') }}

    {% if is_incremental() %}
    where ts_date >= current_date
    {% endif %}
),

-- Latest per-device consumption forecast rows: the fallback estimate AND the
-- liveness gate for the baseline path.
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

-- v3 marker: devices with shift_potential rows have >= min_history_days of history.
-- (settlement rows are NOT the marker — they drive payments and are never gated.)
baseline_devices as (
    select distinct device_id
    from {{ ref('rec_device_baselines') }}
    where baseline_type = 'shift_potential'
),

-- Liveness: at least one usable forecast row inside the window.
live_device_windows as (
    select distinct
        df.device_id,
        w.ts_date,
        w.window_start,
        w.window_end,
        w.community_kwh,
        w.confidence
    from windows w
    join device_forecasts df
      on df.ts >= w.window_start
     and df.ts < w.window_end
),

-- Baseline path: expand each (device, window) into 15-min slots and integrate the
-- per-slot baselines. Same generate_series/slot-key pattern as rec_flexibility_bonus.
window_slots as (
    select
        ldw.device_id,
        ldw.ts_date,
        ldw.window_start,
        ldw.window_end,
        ldw.community_kwh,
        ldw.confidence,
        slot_ts
    from live_device_windows ldw
    join baseline_devices bd using (device_id)
    cross join lateral generate_series(
        ldw.window_start,
        ldw.window_end - interval '15 minutes',
        interval '15 minutes'
    ) as slot_ts
),

baseline_estimates as (
    select
        ws.device_id,
        ws.ts_date,
        ws.window_start,
        ws.window_end,
        max(ws.community_kwh)                                        as community_kwh,
        max(ws.confidence)                                           as confidence,
        -- expected: what the device typically consumes in these slots (floor mirrors
        -- the settlement model's 0.025 kWh/15min minimum baseline)
        sum(coalesce(bs.baseline_kwh, 0.025))                        as expected_kwh,
        -- potential: how much MORE it has demonstrably been able to consume
        sum(coalesce(bp.baseline_kwh, 0.0))                          as potential_kwh,
        -- M1-only cap: export headroom + import spread
        sum(coalesce(be.baseline_kwh, 0.0) + coalesce(bi.baseline_kwh, 0.0)) as cap_kwh
    from window_slots ws
    left join {{ ref('rec_device_baselines') }} bs
      on  bs.device_id     = ws.device_id
     and  bs.baseline_type = 'settlement'
     and  bs.slot = extract(hour from ws.slot_ts) * 4
                    + extract(minute from ws.slot_ts)::int / 15
     and  bs.is_weekday = (extract(dow from ws.slot_ts) between 1 and 5)
    left join {{ ref('rec_device_baselines') }} bp
      on  bp.device_id     = ws.device_id
     and  bp.baseline_type = 'shift_potential'
     and  bp.slot = extract(hour from ws.slot_ts) * 4
                    + extract(minute from ws.slot_ts)::int / 15
     and  bp.is_weekday = (extract(dow from ws.slot_ts) between 1 and 5)
    left join {{ ref('rec_device_baselines') }} be
      on  be.device_id     = ws.device_id
     and  be.baseline_type = 'grid_export_median'
     and  be.slot = extract(hour from ws.slot_ts) * 4
                    + extract(minute from ws.slot_ts)::int / 15
     and  be.is_weekday = (extract(dow from ws.slot_ts) between 1 and 5)
    left join {{ ref('rec_device_baselines') }} bi
      on  bi.device_id     = ws.device_id
     and  bi.baseline_type = 'import_spread'
     and  bi.slot = extract(hour from ws.slot_ts) * 4
                    + extract(minute from ws.slot_ts)::int / 15
     and  bi.is_weekday = (extract(dow from ws.slot_ts) between 1 and 5)
    group by ws.device_id, ws.ts_date, ws.window_start, ws.window_end
),

device_windows_baseline as (
    select
        be.device_id,
        be.ts_date,
        be.window_start,
        be.window_end,
        be.community_kwh,
        be.confidence,
        (
            be.expected_kwh
            + case when coalesce(dc.is_m1_only, false)
                   then least(be.potential_kwh, be.cap_kwh)
                   else be.potential_kwh
              end
        ) * {{ var('calibration_lambda', 1.0) }}                     as estimated_kwh
    from baseline_estimates be
    left join {{ ref('rec_device_class') }} dc using (device_id)
),

-- Cold-start fallback: live devices without shift_potential baselines keep the
-- legacy forecast-based estimate.
device_windows_fallback as (
    select
        ldw.device_id,
        ldw.ts_date,
        ldw.window_start,
        ldw.window_end,
        max(ldw.community_kwh)      as community_kwh,
        max(ldw.confidence)         as confidence,
        sum(df.consumption_kwh)     as estimated_kwh
    from live_device_windows ldw
    join device_forecasts df
      on df.device_id = ldw.device_id
     and df.ts >= ldw.window_start
     and df.ts < ldw.window_end
    where ldw.device_id not in (select device_id from baseline_devices)
    group by ldw.device_id, ldw.ts_date, ldw.window_start, ldw.window_end
),

device_windows as (
    select device_id, ts_date, window_start, window_end, community_kwh, confidence, estimated_kwh
    from device_windows_baseline
    union all
    select device_id, ts_date, window_start, window_end, community_kwh, confidence, estimated_kwh
    from device_windows_fallback
),

-- Proportional cap so the sum of device estimates never exceeds community_kwh.
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
    round(
        case
            when wt.total_device_kwh > dw.community_kwh
            then dw.estimated_kwh / wt.total_device_kwh * dw.community_kwh
            else dw.estimated_kwh
        end
    ::numeric, 2)                                                                           as estimated_kwh,
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
    dw.confidence::numeric                                                                  as confidence,
    '{{ var("flexibility_model", "solar_overproduction") }}'::text                          as flexibility_model
from device_windows dw
join window_totals wt using (window_start, window_end)
where dw.estimated_kwh > 0
