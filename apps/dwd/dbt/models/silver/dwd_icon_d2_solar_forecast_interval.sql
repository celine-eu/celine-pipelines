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
    on_schema_change = 'sync_all_columns'
) }}

with base as (

    select
        -- authoritative time semantics from the tap
        run_datetime::timestamptz            as run_time_utc,
        interval_start_datetime::timestamptz as interval_start_utc,
        interval_end_datetime::timestamptz   as interval_end_utc,

        lat::double precision as lat,
        lon::double precision as lon,

        name,
        value::double precision as value,

        _sdc_last_modified
    from {{ source('raw', 'dwd_icon_d2') }}
    where name = 'avg_snswrf'

    {% if is_incremental() %}
      and run_datetime >= (
          select coalesce(max(run_time_utc), '1970-01-01')
          from {{ this }}
      )
    {% endif %}
),

dedup as (
    select
        *,
        row_number() over (
            partition by
                run_time_utc,
                interval_start_utc,
                interval_end_utc,
                lat,
                lon
            order by _sdc_last_modified desc
        ) as rn
    from base
)

select
    run_time_utc,
    interval_start_utc,
    interval_end_utc,

    lat,
    lon,

    name,
    value,

    -- authoritative interval duration (hours)
    extract(epoch from (interval_end_utc - interval_start_utc)) / 3600.0
        as interval_hours,

    _sdc_last_modified
from dedup
where rn = 1
