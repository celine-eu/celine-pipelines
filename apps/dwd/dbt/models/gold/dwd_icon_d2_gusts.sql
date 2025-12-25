{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['run_time_utc','date','lat','lon'],
    on_schema_change = 'sync_all_columns',
    schema = 'gold'
) }}

{% set WARNING = 7.62 %}
{% set ALERT = 12.46 %}

with base as (

    select
        run_time_utc,
        interval_end_utc,
        lat,
        lon,
        wind_speed,
        i10fg,
        _sdc_last_modified
    from {{ ref('dwd_icon_d2_wind') }}

    {% if is_incremental() %}
      where run_time_utc >= (
          select coalesce(max(run_time_utc), '1970-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

daily as (
    select
        run_time_utc,
        date_trunc('day', interval_end_utc)::date as date,
        lat,
        lon,

        max(wind_speed) as wind_speed_max,
        max(i10fg)      as i10fg_max,

        max(_sdc_last_modified) as _sdc_last_modified
    from base
    group by
        run_time_utc,
        date,
        lat,
        lon
),

final as (
    select
        run_time_utc,
        date,
        lat,
        lon,

        ST_SetSRID(
            ST_MakePoint(lon::double precision, lat::double precision),
            4326
        )::geography as geoposition,

        wind_speed_max,
        i10fg_max,

        (i10fg_max - wind_speed_max) as gust_excess,

        case
            when (i10fg_max - wind_speed_max) >= {{ ALERT }}   then 'ALERT'
            when (i10fg_max - wind_speed_max) >= {{ WARNING }} then 'WARNING'
            else 'NORMAL'
        end as gust_excess_tier,

        _sdc_last_modified
    from daily
)

select * from final
