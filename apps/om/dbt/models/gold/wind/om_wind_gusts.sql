{{ config(
    materialized         = 'incremental',
    unique_key           = ['date', 'lat', 'lon'],
    incremental_strategy = 'merge'
) }}

{#
    Gold layer: daily wind gust alerts per grid point.

    The day is the worst of its three 8-hour windows (om_wind_gusts_8h): the
    daily gust excess is the max window excess and the tier follows from it,
    so the daily map is, by construction, the union of the intra-day windows.

    Rationale (2026-09-11): gust excess is max gust minus max sustained wind,
    and on a whole day the two maxima can fall hours apart — a strong steady
    wind in the evening used to cancel a gusty morning (in March 2026 data 65
    of 3,144 cell-days were NORMAL while one window was at WARNING/ALERT).
    Taking the worst window keeps the thresholds and changes ~2% of cell-days,
    always upward.

    wind_speed_max / wind_gusts_max stay the daily maxima; the averages are the
    hour-weighted means of the window averages. gust_excess is therefore no
    longer wind_gusts_max - wind_speed_max.

    Thresholds inherited from DWD ICON-D2 gust calibration:
      WARNING >= 7.62 m/s gust excess
      ALERT   >= 12.46 m/s gust excess

    Incremental lookback: forecast hours are refreshed on every run, so
    anchoring on max(date) alone would freeze each day at its first (oldest)
    forecast. The last 3 days are recomputed on every run.
#}

{% set WARNING = 7.62 %}
{% set ALERT = 12.46 %}

with windows as (

    select *
    from {{ ref('om_wind_gusts_8h') }}

    {% if is_incremental() %}
    where date >= (
        select coalesce(max(date), '1970-01-01'::date)
        from {{ this }}
    ) - 3
    {% endif %}

),

daily as (

    select
        date,
        lat,
        lon,

        max(wind_speed_max)     as wind_speed_max,
        max(wind_gusts_max)     as wind_gusts_max,
        sum(wind_speed_avg * n_hours) / nullif(sum(n_hours), 0) as wind_speed_avg,
        sum(wind_gusts_avg * n_hours) / nullif(sum(n_hours), 0) as wind_gusts_avg,

        max(gust_excess)        as gust_excess,

        max(_sdc_extracted_at)  as _sdc_extracted_at
    from windows
    group by date, lat, lon

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

        wind_speed_max,
        wind_gusts_max,
        wind_speed_avg,
        wind_gusts_avg,

        gust_excess,

        case
            when gust_excess >= {{ ALERT }}   then 'ALERT'
            when gust_excess >= {{ WARNING }} then 'WARNING'
            else 'NORMAL'
        end as gust_excess_tier,

        _sdc_extracted_at
    from daily

)

select * from final
