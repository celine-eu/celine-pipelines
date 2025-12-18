{{ config(
    materialized='incremental',
    unique_key='datetime_utc'
) }}

with base as (
    select
        cast(toa as float)                 as toa,
        cast(clear_sky_ghi as float)       as clear_sky_ghi,
        cast(clear_sky_bhi as float)       as clear_sky_bhi,
        cast(clear_sky_dhi as float)       as clear_sky_dhi,
        cast(clear_sky_bni as float)       as clear_sky_bni,

        cast(ghi as float)                 as ghi,
        cast(bhi as float)                 as bhi,
        cast(dhi as float)                 as dhi,
        cast(bni as float)                 as bni,

        cast(reliability as float)         as reliability,

        -- normalize datetime to UTC timestamp
        cast(date_start as timestamp)      as datetime_utc
    from {{ ref('stg_radiation_raw') }}
    where date_start is not null
)

, enriched as (
    select
        datetime_utc,
        toa,
        clear_sky_ghi,
        clear_sky_bhi,
        clear_sky_dhi,
        clear_sky_bni,
        ghi,
        bhi,
        dhi,
        bni,
        reliability,

        -- useful breakdowns
        date_trunc('day', datetime_utc)    as date_day,
        extract(hour from datetime_utc)    as hour_utc,
        case
            when ghi > 0 then ghi/clear_sky_ghi
            else null
        end as ghi_clearness_index
    from base
)

select * from enriched
