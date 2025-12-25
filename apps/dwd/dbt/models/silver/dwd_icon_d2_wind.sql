{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = [
        'run_time_utc',
        'interval_start_utc',
        'interval_end_utc',
        'lat',
        'lon'
    ],
    on_schema_change = 'sync_all_columns',
    schema = 'silver'
) }}

with base as (

    select
        run_time_utc,
        interval_start_utc,
        interval_end_utc,
        lat,
        lon,
        name,
        value,
        _sdc_last_modified
    from {{ ref('stg_dwd_icon_d2') }}
    where name in ('10u', '10v', 'max_i10fg')

    {% if is_incremental() %}
      and run_time_utc >= (
          select coalesce(max(run_time_utc), '1970-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}
),

pivoted as (
    select
        run_time_utc,
        interval_start_utc,
        interval_end_utc,
        lat,
        lon,

        max(case when name = '10u'       then value end) as u10,
        max(case when name = '10v'       then value end) as v10,
        max(case when name = 'max_i10fg' then value end) as i10fg,

        max(_sdc_last_modified) as _sdc_last_modified
    from base
    group by
        run_time_utc,
        interval_start_utc,
        interval_end_utc,
        lat,
        lon
)

select
    run_time_utc,
    interval_start_utc,
    interval_end_utc,
    lat,
    lon,

    u10,
    v10,
    i10fg,

    case
        when u10 is not null and v10 is not null
            then sqrt(power(u10, 2) + power(v10, 2))::double precision
        else null
    end as wind_speed,

    _sdc_last_modified
from pivoted
