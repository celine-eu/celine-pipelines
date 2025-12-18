{{ config(
    materialized = 'incremental',
    unique_key = ['datetime','lat','lon'],
    incremental_strategy = 'merge',
    schema = 'silver'
) }}

with base as (

    select *
    from {{ ref('stg_dwd_icon_d2') }}

    {% if is_incremental() %}
    where base_datetime > (
            select coalesce(max(base_datetime), '1970-01-01')
            from {{ this }}
        )
    {% endif %}
),

pivoted as (
    select
        datetime,
        base_datetime,
        lat,
        lon,

        max(case when name = '10u'   then value end) as u10,
        max(case when name = '10v'   then value end) as v10,
        max(case when name = 'max_i10fg' then value end) as i10fg,

        max(_sdc_last_modified) as _sdc_last_modified
    from base
    group by
        datetime, base_datetime, lat, lon
),

enhanced as (
    select
        datetime,
        base_datetime,
        lat,
        lon,
        u10,
        v10,
        i10fg,
        sqrt(power(u10,2) + power(v10,2))::float as wind_speed,
        _sdc_last_modified
    from pivoted
)

select * from enhanced