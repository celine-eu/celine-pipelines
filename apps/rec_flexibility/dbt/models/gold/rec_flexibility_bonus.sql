{{
    config(
        materialized='incremental',
        unique_key='_id',
        incremental_strategy='merge',
        merge_update_columns=[
            'device_id', 'window_start', 'window_end',
            'shifted_kwh', 'reference_kwh', 'shift_fraction',
            'shift_effort_mult', 'event_mult', 'accuracy', 'streak_mult',
            'bonus_points_raw', 'bonus_points_capped', 'bonus_points'
        ],
        post_hook=[
            "delete from {{ this }} b where not exists (
                select 1 from {{ ref('silver_flexibility_commitments') }} c
                where c.device_id = b.device_id
                  and b.window_start >= c.period_start
                  and b.window_end   <= c.period_end
                  and c.status in ('committed', 'settled')
            )"
        ]
    )
}}

-- Flexibility bonus per (committed device, window). Gated by silver_flexibility_commitments
-- so that only devices with status in ('committed','settled') on that window earn a bonus
-- (product decision: the bonus rewards the contractual act of commitment, not passive
-- response). The bare-token status filter relies on Task 0 Step 0.7's split_part
-- normalisation at silver.

with committed_windows as (
    select
        device_id,
        period_start as window_start,
        period_end   as window_end
    from {{ ref('silver_flexibility_commitments') }}
    where status in ('committed', 'settled')
),
windows as (
    select distinct
        w.device_id, w.window_start, w.window_end, fw.ts_date, fw.community_kwh
    from committed_windows w
    join {{ ref('rec_flexibility_windows') }} fw
      on fw.device_id    = w.device_id
     and fw.window_start = w.window_start
     and fw.window_end   = w.window_end
    {% if is_incremental() %}
    where w.window_start >= date_trunc('day', now() - interval '7 days')
    {% endif %}
),
window_intervals as (
    select
        s.device_id,
        w.window_start,
        w.window_end,
        w.ts_date,
        w.community_kwh,
        sum(s.consumption_kwh) as actual_kwh,
        count(*) as n_intervals,
        sum(case when s.is_surplus_interval = 1 then 1 else 0 end) as actual_surplus_intervals
    from windows w
    join {{ ref('rec_settlement_points') }} s
      on s.device_id = w.device_id
     and s.ts >= w.window_start and s.ts < w.window_end
    group by s.device_id, w.window_start, w.window_end, w.ts_date, w.community_kwh
),
-- Reference baseline integrated over the window. Mirrors gamification/03_flexibility_bonus.ipynb
-- cell 3: bl_kwh = sum over every 15-min slot inside [window_start, window_end) of the per-slot
-- reference baseline (already kWh per 15-min bucket). Never sample only window_start.
window_slots as (
    select
        wi.device_id,
        wi.window_start,
        wi.window_end,
        slot_ts
    from window_intervals wi
    cross join lateral generate_series(
        wi.window_start,
        wi.window_end - interval '15 minutes',
        interval '15 minutes'
    ) as slot_ts
),
reference_per_window as (
    select
        ws.device_id,
        ws.window_start,
        ws.window_end,
        coalesce(sum(b.baseline_kwh), 0) as reference_kwh
    from window_slots ws
    left join {{ ref('rec_device_baselines') }} b
      on  b.device_id     = ws.device_id
     and  b.baseline_type = 'reference'
     and  b.slot = extract(hour from ws.slot_ts) * 4
                   + extract(minute from ws.slot_ts)::int / 15
     and  b.is_weekday = (extract(dow from ws.slot_ts) between 1 and 5)
    group by ws.device_id, ws.window_start, ws.window_end
),
shift_calc as (
    select
        wi.*,
        coalesce(rpw.reference_kwh, 0) as reference_kwh,
        greatest(wi.actual_kwh - coalesce(rpw.reference_kwh, 0), 0) as shifted_kwh,
        case
            when coalesce(rpw.reference_kwh, 0) <= 0 then 0
            else greatest(wi.actual_kwh - rpw.reference_kwh, 0) / rpw.reference_kwh
        end as shift_fraction
    from window_intervals wi
    left join reference_per_window rpw using (device_id, window_start, window_end)
),
multipliers as (
    select
        sc.*,
        case
            when sc.shift_fraction < 0.05 or sc.shifted_kwh < 0.05 then 0.0
            when sc.shift_fraction >= 1.00 then 2.5
            when sc.shift_fraction >= 0.50 then 2.0
            when sc.shift_fraction >= 0.25 then 1.5
            else 1.0
        end as shift_effort_mult,
        1.0 as event_mult,
        greatest(0.5, sc.actual_surplus_intervals::float / nullif(sc.n_intervals, 0))
            as accuracy
    from shift_calc sc
),
with_streak as (
    select
        m.*,
        coalesce(s.multiplier, 1.0) as streak_mult
    from multipliers m
    left join {{ ref('rec_device_streaks') }} s using (device_id)
),
raw_bonus as (
    select
        ws.*,
          ws.shifted_kwh * 15 * ws.shift_effort_mult
        * ws.event_mult * ws.accuracy * ws.streak_mult as bonus_points_raw
    from with_streak ws
),
device_30d_avg as (
    select device_id, avg(bonus_points_raw) as avg_30d
    from raw_bonus
    where window_start >= now() - interval '30 days'
    group by device_id
),
capped as (
    select
        rb.*,
        case
            when da.avg_30d is null or da.avg_30d = 0 then rb.bonus_points_raw
            else least(rb.bonus_points_raw, da.avg_30d * 2.0)
        end as bonus_points_capped
    from raw_bonus rb
    left join device_30d_avg da using (device_id)
)
select
    md5(device_id || window_start::text || window_end::text) as _id,
    device_id,
    window_start,
    window_end,
    shifted_kwh,
    reference_kwh,
    shift_fraction,
    shift_effort_mult,
    event_mult,
    accuracy,
    streak_mult,
    bonus_points_raw,
    bonus_points_capped,
    round(bonus_points_capped)::int as bonus_points
from capped
where shifted_kwh > 0
