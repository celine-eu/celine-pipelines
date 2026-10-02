{{ config(
    materialized         = 'incremental',
    unique_key           = ['date', 'lat', 'lon'],
    incremental_strategy = 'merge'
) }}

{#
    Gold layer: daily soil heat risk per grid point.

    7-day trailing mean of soil_1m_c per (lat, lon), only considered
    "complete" (soil_data_complete) when the window holds exactly 7 rows
    spanning exactly 6 days -- i.e. 7 consecutive daily readings with no
    gaps. soil7_mean_c and soil_status are NULL unless the window is
    complete.

    soil_status is ORANGE when the window is complete, the month is
    May-Sep, and soil7_mean_c exceeds the P90 threshold (default 21.4832,
    calibrated on the Trento valley floor series, 2014-2025, notebook 13
    of the thermal model, and applied to every grid point -- ERA5-Land
    cells are ~9km, so neighbouring points share values); GREEN otherwise
    while the window is complete.

    TWO-BAND INCREMENTAL DESIGN (do not collapse this to one band).
    The 7-day trailing window needs 6 days of history *before* whatever
    date we intend to emit. A naive single-band incremental filter (read
    only the last N days, from om_soil_daily, and emit all of them) would
    recompute the oldest dates in that band with a truncated window
    (fewer than 7 preceding rows visible), overwrite their previously
    correct soil_data_complete / soil7_mean_c / soil_status with
    false / NULL / NULL via the merge, and then those dates age out of
    the band on the *next* run without ever being repaired: a silent,
    permanent loss of otherwise-correct data.

    So on incremental runs, `cutoffs` computes two bounds once from a
    single scan of {{ this }}:
      - read_from  (current_max_date - 16 days): the READ band for
        `base`, wide enough that every date we might emit still has its
        full 6 preceding days available to the window function
        (10-day emit band + 6-day window lookback).
      - emit_from  (current_max_date - 10 days): the EMIT band, applied
        as the final filter on `final` -- this is what actually gets
        merged into {{ this }}, same width as before this fix.
    Both `base` and the final select reference the same `cutoffs` CTE,
    so the two bounds can never drift apart.
#}

with

{% if is_incremental() %}
cutoffs as (

    select
        (coalesce(max(date), '1970-01-01'::date) - interval '16 days')::date as read_from,
        (coalesce(max(date), '1970-01-01'::date) - interval '10 days')::date as emit_from
    from {{ this }}

),
{% endif %}

base as (

    select *
    from {{ ref('om_soil_daily') }}

    {% if is_incremental() %}
    where date >= (select read_from from cutoffs)
    {% endif %}

),

windowed as (

    select
        *,
        avg(soil_1m_c) over w   as soil7_mean_c_raw,
        count(*) over w         as soil7_n_days,
        min(date) over w        as soil7_window_start
    from base
    window w as (
        partition by lat, lon
        order by date
        rows between 6 preceding and current row
    )

),

final as (

    select

        date,
        lat,
        lon,

        ST_SetSRID(
            ST_MakePoint(lon::double precision, lat::double precision),
            4326
        )::geography as geoposition,

        soil_1m_c,

        case
            when soil7_n_days = 7 and (date - soil7_window_start) = 6
                then soil7_mean_c_raw
        end as soil7_mean_c,

        {{ var('soil7_p90_c', 21.4832) }} as soil7_p90_c,

        soil7_n_days,

        (soil7_n_days = 7 and (date - soil7_window_start) = 6) as soil_data_complete,

        case
            when soil7_n_days != 7 or (date - soil7_window_start) != 6 then null
            when extract(month from date) between 5 and 9
                 and soil7_mean_c_raw > {{ var('soil7_p90_c', 21.4832) }}
                then 'ORANGE'
            else 'GREEN'
        end as soil_status,

        _sdc_extracted_at

    from windowed

)

select *
from final
{% if is_incremental() %}
where date >= (select emit_from from cutoffs)
{% endif %}
