{#
  Community-level flexibility windows — the EVENT layer of the event/enrichment split.
  One row per community surplus window. Row existence = the opportunity is visible to
  ALL community members. Per-device estimates live in rec_flexibility_windows.

  The incremental pre-hook deletes the refresh scope (ts_date >= current_date) before
  merging, so windows whose bounds shifted between forecast generations cannot leave
  stale rows behind (previously required a manual --full-refresh).
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
      'confidence',
      'flexibility_model'
    ],
    pre_hook="{% if is_incremental() %}delete from {{ this }} where ts_date >= current_date{% endif %}"
  )
}}

-- Community surplus threshold (kWh for a 1-hour period, community aggregate)
{% set EXPORT_THRESHOLD_KWH = 0.5 %}
-- Maximum hours per suggestion window
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
      and f.timestamp::timestamp::date >= current_date
      {% endif %}
),

windowed as (
    select
        ts,
        ts_date,
        net_exchange_kwh,
        lag(ts) over (partition by ts_date order by ts) as prev_ts
    from surplus_forecast
),

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
)

select
    md5(w.ts_date::text || w.window_start::text || w.window_end::text)  as _id,
    w.ts_date,
    w.window_start,
    w.window_end,
    round(w.community_kwh::numeric, 2)                                  as community_kwh,
    -- TODO [CELINE-FLEX-CONF-CAL]: placeholder until calibration seed lands.
    0.75::numeric                                                       as confidence,
    '{{ var("flexibility_model", "solar_overproduction") }}'::text      as flexibility_model
from windows w
