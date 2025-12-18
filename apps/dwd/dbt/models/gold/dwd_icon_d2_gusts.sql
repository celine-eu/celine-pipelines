{{ config(
    materialized = 'incremental',
    unique_key = ['date','lat','lon'],
    incremental_strategy = 'merge',
    schema = 'gold'
) }}

{% set WARNING = 7.62 %}
{% set ALERT = 12.46 %}

with
{% if is_incremental() %}
cutoff as (
    select coalesce(max(base_datetime), '1970-01-01') as last_dt
    from {{ this }}
),
updated_source as (
    select *
    from {{ ref('dwd_icon_d2_wind') }}
    where base_datetime > (select last_dt from cutoff)
),
{% else %}
updated_source as (
    select *
    from {{ ref('dwd_icon_d2_wind') }}
),
{% endif %}

daily as (
    select
        date_trunc('day', datetime)::date as date,
        lat,
        lon,
        max(wind_speed) as wind_speed_max,
        max(i10fg)      as i10fg_max,
        max(base_datetime) as base_datetime
    from updated_source
    group by 1,2,3
),

final as (
    select
        date,
        base_datetime,
        lat,
        lon,
        ST_SetSRID(ST_MakePoint(lon,lat),4326) as geoposition,
        wind_speed_max,
        i10fg_max,
        (i10fg_max - wind_speed_max) as gust_excess,
        case
            when i10fg_max - wind_speed_max >= {{ ALERT }} then 'ALERT'
            when i10fg_max - wind_speed_max >= {{ WARNING }} then 'WARNING'
            else 'NORMAL'
        end as gust_excess_tier
    from daily
)

select * from final