{{ config(
    materialized = 'view',
    schema = 'staging'
) }}

select distinct on (
    run_datetime,
    interval_start_datetime,
    interval_end_datetime,
    lat,
    lon,
    name
)
    -- authoritative time semantics
    run_datetime::timestamptz             as run_time_utc,
    interval_start_datetime::timestamptz  as interval_start_utc,
    interval_end_datetime::timestamptz    as interval_end_utc,

    step_range,
    step_units,

    -- spatial
    lat::double precision                 as lat,
    lon::double precision                 as lon,

    -- variable
    name,
    value::double precision               as value,

    -- metadata
    level_type,
    level,
    ensemble,
    centre,
    data_type,
    grid_type,

    _sdc_filename,
    _sdc_last_modified

from {{ source('raw', 'dwd_icon_d2') }}

-- keep only future-valid intervals
where interval_end_datetime >= current_date

order by
    run_datetime,
    interval_start_datetime,
    interval_end_datetime,
    lat,
    lon,
    name,
    _sdc_last_modified desc
