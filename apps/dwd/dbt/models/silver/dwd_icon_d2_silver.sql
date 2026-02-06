{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = [
        'run_time_utc',
        'forecast_hour',
        'lat',
        'lon',
        'name'
    ],
    on_schema_change = 'sync_all_columns'
) }}

with base as (

    select
        run_datetime::timestamptz            as run_time_utc,
        interval_start_datetime::timestamptz as interval_start_utc,
        interval_end_datetime::timestamptz   as interval_end_utc,

        lat::double precision as lat,
        lon::double precision as lon,

        name,
        value::double precision as value,

        _sdc_last_modified
    from {{ source('raw', 'dwd_icon_d2') }}
    where name in ('avg_snswrf', 'tp', '2t', 'CLCT')

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
                lon,
                name
            order by _sdc_last_modified desc
        ) as rn
    from base
),

enriched as (
    select
        run_time_utc,
        interval_start_utc,
        interval_end_utc,
        lat,
        lon,
        name,
        value,

        -- forecast hour = hours from run init to valid time
        extract(epoch from (interval_end_utc - run_time_utc)) / 3600.0
            as forecast_hour,

        -- interval duration (0 for instant vars, N for accum/avg)
        extract(epoch from (interval_end_utc - interval_start_utc)) / 3600.0
            as interval_hours,

        -- valid datetime in Rome TZ (for downstream filtering)
        interval_end_utc at time zone 'Europe/Rome'
            as valid_datetime_rome,

        -- step type tag for downstream logic
        case name
            when 'avg_snswrf' then 'avg'
            when 'tp'         then 'accum'
            when '2t'         then 'instant'
            when 'CLCT'       then 'instant'
        end as step_type,

        -- previous step's value (for de-accumulation)
        lag(value) over (
            partition by run_time_utc, lat, lon, name
            order by interval_end_utc
        ) as prev_value,

        -- previous step's forecast_hour (needed for avg_snswrf weighted formula)
        lag(
            extract(epoch from (interval_end_utc - run_time_utc)) / 3600.0
        ) over (
            partition by run_time_utc, lat, lon, name
            order by interval_end_utc
        ) as prev_forecast_hour,

        _sdc_last_modified

    from dedup
    where rn = 1
)

select * from enriched