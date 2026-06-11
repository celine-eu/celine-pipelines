{{
    config(
        materialized='incremental',
        unique_key='_id',
        incremental_strategy='merge',
        merge_update_columns=[
            'device_id', 'flag_date', 'flag_type',
            'metric_value', 'threshold_value', 'severity'
        ]
    )
}}

-- L3: per-window bonus exceeded the 2x rolling-avg cap (recorded as the gap between
--     bonus_points_raw and bonus_points_capped in rec_flexibility_bonus).
-- L4: daily bonus points > 10x the device's 30-day rolling median.

with daily_bonus as (
    select
        device_id,
        date_trunc('day', window_start)::date as flag_date,
        sum(bonus_points) as daily_points
    from {{ ref('rec_flexibility_bonus') }}
    {% if is_incremental() %}
    where window_start >= date_trunc('day', now() - interval '7 days')
    {% endif %}
    group by device_id, date_trunc('day', window_start)::date
),
median_30d as (
    select
        db.device_id,
        db.flag_date,
        db.daily_points,
        m.median_30d
    from daily_bonus db
    left join lateral (
        select percentile_cont(0.5) within group (order by prev.daily_points) as median_30d
        from daily_bonus prev
        where prev.device_id = db.device_id
          and prev.flag_date >= db.flag_date - interval '30 days'
          and prev.flag_date < db.flag_date
    ) m on true
),
spike_flags as (
    select
        device_id,
        flag_date,
        'L4_DAILY_SPIKE' as flag_type,
        daily_points as metric_value,
        median_30d * 10 as threshold_value,
        'warning' as severity
    from median_30d
    where median_30d is not null
      and median_30d > 0
      and daily_points > median_30d * 10
),
window_caps as (
    select
        device_id,
        window_start,
        date_trunc('day', window_start)::date as flag_date,
        'L3_WINDOW_CAP' as flag_type,
        bonus_points_raw as metric_value,
        bonus_points_capped as threshold_value,
        'info' as severity
    from {{ ref('rec_flexibility_bonus') }}
    where bonus_points_raw > bonus_points_capped + 0.001
)
select md5(device_id || flag_date::text || flag_type) as _id,
       device_id, flag_date, flag_type, metric_value, threshold_value, severity
from spike_flags
union all
select md5(device_id || flag_date::text || flag_type || window_start::text) as _id,
       device_id, flag_date, flag_type, metric_value, threshold_value, severity
from window_caps
