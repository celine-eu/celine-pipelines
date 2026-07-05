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
    ],
    pre_hook="{% if is_incremental() %}delete from {{ this }} where ts_date >= current_date{% endif %}"
  )
}}

-- Per-device flexibility window enrichment.
--
-- Detection logic (surplus threshold, window grouping/slicing, sleeping-hour
-- exclusion) now lives in rec_flexibility_windows_community — the EVENT layer.
-- This model is the per-device ENRICHMENT layer: it cross-joins the community
-- windows with per-device consumption forecasts to compute personalized
-- estimated_kwh (how much community solar the device could absorb by shifting
-- load into the window), then proportionally normalizes so the sum of device
-- estimates never exceeds the community's available surplus.

with windows as (
    select
        ts_date,
        window_start,
        window_end,
        community_kwh
    from {{ ref('rec_flexibility_windows_community') }}

    {% if is_incremental() %}
    where ts_date >= current_date
    {% endif %}
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
    0.75::numeric                                           as confidence,
    '{{ var("flexibility_model", "solar_overproduction") }}'::text  as flexibility_model
from device_windows dw
join window_totals wt using (window_start, window_end)
where dw.estimated_kwh > 0
