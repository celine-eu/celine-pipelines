{{ config(materialized='view', tags=['rec_flexibility', 'fair']) }}

-- Fair gamification: the M1 grid-exchange channel of every community POD, one row per
-- (device_id, ts), with the local-time features every fair model needs.
--
-- Units: import_kwh / export_kwh are kWh per 15-minute bucket (silver meters_data
-- consumption_kwh / production_kwh of the M1 meter). One device stores two rows per
-- quarter, so the channel is collapsed with max(), never sum().
-- Calendar: every local feature uses Europe/Rome (var fair_tz). Night is 21:00–06:00
-- (36 quarters; 40 on the DST fall-back day), midday 11:00–15:00 (hidden-PV test).
-- slot = quarter-of-day index 0–95 in local time.

with m1 as (
    select
        device_id,
        ts,
        max(consumption_kwh) as import_kwh,
        max(production_kwh)  as export_kwh
    from {{ source('metering_silver', 'meters_data') }}
    where meter_type = 'M1'
      and ts >= '{{ var("fair_history_start") }}'::date
    group by device_id, ts
),
local as (
    select
        m.*,
        (m.ts at time zone '{{ var("fair_tz") }}') as local_ts
    from m1 m
)
select
    l.device_id,
    r.community_id,
    r.substation_id,
    l.ts,
    l.local_ts,
    l.local_ts::date                                                                     as local_day,
    (extract(hour from l.local_ts) * 4 + floor(extract(minute from l.local_ts) / 15))::int as slot,
    (extract(isodow from l.local_ts) <= 5)                                               as is_weekday,
    (l.local_ts::time >= '{{ var("fair_night_start") }}'::time
        or l.local_ts::time < '{{ var("fair_night_end") }}'::time)                       as is_night,
    (l.local_ts::time >= '{{ var("fair_midday_start") }}'::time
        and l.local_ts::time < '{{ var("fair_midday_end") }}'::time)                     as is_midday,
    l.import_kwh,
    l.export_kwh
from local l
join {{ ref('rec_fair_device_registry') }} r using (device_id)
