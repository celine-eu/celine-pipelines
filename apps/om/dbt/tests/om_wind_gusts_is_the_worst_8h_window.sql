-- The daily gust excess (and tier) is the worst of the day's 8-hour windows, so
-- the daily map is the union of the intra-day windows by construction.
--
-- Scoped to the dates every incremental run recomputes (max(date) - 3, the
-- model's lookback): older rows were written under the previous whole-day
-- formula and are kept as history, not rebuilt.
with worst as (
    select date, lat, lon, max(gust_excess) as excess
    from {{ ref('om_wind_gusts_8h') }}
    group by 1, 2, 3
)
select d.date, d.lat, d.lon, d.gust_excess, w.excess as worst_window_excess
from {{ ref('om_wind_gusts') }} d
join worst w using (date, lat, lon)
where d.date >= (select max(date) from {{ ref('om_wind_gusts') }}) - 3
  and abs(d.gust_excess - w.excess) > 1e-9
