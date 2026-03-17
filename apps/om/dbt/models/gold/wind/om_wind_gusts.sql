{{ config(
    materialized         = 'incremental',
    unique_key           = ['date', 'lat', 'lon'],
    incremental_strategy = 'merge'
) }}

{#
    Gold layer: daily wind gust alerts per grid point.
    Aggregates hourly wind data to daily max/avg, computes gust excess
    (max_gust - max_sustained_wind), and classifies into alert tiers.
    Includes PostGIS geography point for spatial queries.

    Thresholds inherited from DWD ICON-D2 gust calibration:
      WARNING >= 7.62 m/s gust excess
      ALERT   >= 12.46 m/s gust excess
#}

{% set WARNING = 7.62 %}
{% set ALERT = 12.46 %}

with base as (

    select *
    from {{ ref('om_wind_hourly') }}

    {% if is_incremental() %}
    where datetime >= (
        select coalesce(max(date), '1970-01-01'::date)::timestamp
        from {{ this }}
    )
    {% endif %}

),

daily as (

    select
        date_trunc('day', datetime)::date  as date,
        lat,
        lon,

        max(wind_speed_ms)     as wind_speed_max,
        max(wind_gusts_ms)     as wind_gusts_max,
        avg(wind_speed_ms)     as wind_speed_avg,
        avg(wind_gusts_ms)     as wind_gusts_avg,

        max(_sdc_extracted_at) as _sdc_extracted_at
    from base
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

        (wind_gusts_max - wind_speed_max) as gust_excess,

        case
            when (wind_gusts_max - wind_speed_max) >= {{ ALERT }}   then 'ALERT'
            when (wind_gusts_max - wind_speed_max) >= {{ WARNING }} then 'WARNING'
            else 'NORMAL'
        end as gust_excess_tier,

        _sdc_extracted_at
    from daily

)

select * from final
