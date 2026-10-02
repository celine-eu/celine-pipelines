{{ config(
    materialized         = 'incremental',
    unique_key           = ['window_start', 'lat', 'lon'],
    incremental_strategy = 'merge'
) }}

{#
    Gold layer: wind gust alerts per grid point on 8-hour windows
    (00–08, 08–16, 16–24 local calendar of the hourly timestamps).

    Same model as om_wind_gusts — max/avg over the window, gust excess
    (max_gust - max_sustained_wind), same DWD ICON-D2 thresholds — only the
    aggregation window changes. om_wind_gusts (the daily table) is derived
    from this one as the worst window of the day, so the daily map is the
    union of the windows by construction.

    Incremental lookback: forecast hours are refreshed on every run, so
    anchoring on max(window_start) alone would freeze each window at its
    first (oldest) forecast. The last 72 h (a multiple of 8, so whole
    windows) are re-aggregated on every run.
#}

{% set WARNING = 7.62 %}
{% set ALERT = 12.46 %}
{% set HOURS = 8 %}

with base as (

    select
        *,
        date_trunc('day', datetime)
            + (floor(extract(hour from datetime) / {{ HOURS }}) * {{ HOURS }}) * interval '1 hour'
            as window_start
    from {{ ref('om_wind_hourly') }}

    {% if is_incremental() %}
    where datetime >= (
        select coalesce(max(window_start), '1970-01-01'::timestamp)
        from {{ this }}
    ) - interval '72 hours'
    {% endif %}

),

windowed as (

    select
        window_start,
        window_start::date                          as date,
        (extract(hour from window_start) / {{ HOURS }})::int as slot,
        lat,
        lon,

        max(wind_speed_ms)     as wind_speed_max,
        max(wind_gusts_ms)     as wind_gusts_max,
        avg(wind_speed_ms)     as wind_speed_avg,
        avg(wind_gusts_ms)     as wind_gusts_avg,
        count(*)               as n_hours,

        max(_sdc_extracted_at) as _sdc_extracted_at
    from base
    group by window_start, lat, lon

),

final as (

    select
        window_start,
        date,
        slot,
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
        n_hours,

        (wind_gusts_max - wind_speed_max) as gust_excess,

        case
            when (wind_gusts_max - wind_speed_max) >= {{ ALERT }}   then 'ALERT'
            when (wind_gusts_max - wind_speed_max) >= {{ WARNING }} then 'WARNING'
            else 'NORMAL'
        end as gust_excess_tier,

        _sdc_extracted_at
    from windowed

)

select * from final
