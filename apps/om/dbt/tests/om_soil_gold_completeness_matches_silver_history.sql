-- For every (lat, lon), any date whose trailing 7-day window in
-- om_soil_daily (silver, full history, not bounded by any incremental
-- read/emit band) is complete -- 7 consecutive daily rows spanning
-- exactly 6 days, no gaps -- must show soil_data_complete = true (and a
-- non-null soil7_mean_c/soil_status) in om_soil_heat_risk (gold).
--
-- This is deliberately independent of om_soil_status_requires_seven_days
-- (which only checks gold's internal consistency: soil_data_complete
-- agrees with soil7_mean_c/soil_status on the SAME row). That test alone
-- cannot catch an incremental-window regression where the gold model's
-- read band is too narrow: gold would happily compute
-- soil_data_complete = false for a date that silver's full history shows
-- should be complete, and the internal-consistency test passes because
-- soil7_mean_c is NULL to match. Comparing gold against silver's full
-- history is what catches that.

with silver_windowed as (

    select
        date,
        lat,
        lon,
        count(*) over w as n_days,
        min(date) over w as window_start
    from {{ ref('om_soil_daily') }}
    window w as (
        partition by lat, lon
        order by date
        rows between 6 preceding and current row
    )

),

expected_complete as (

    select date, lat, lon
    from silver_windowed
    where n_days = 7
      and (date - window_start) = 6

)

select
    e.date,
    e.lat,
    e.lon,
    g.soil_data_complete,
    g.soil7_mean_c,
    g.soil_status
from expected_complete e
join {{ ref('om_soil_heat_risk') }} g
    using (date, lat, lon)
where g.soil_data_complete is distinct from true
   or g.soil7_mean_c is null
   or g.soil_status is null
