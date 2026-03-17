{{ config(
    materialized         = 'incremental',
    unique_key           = ['datetime', 'lat', 'lon'],
    incremental_strategy = 'merge'
) }}

{#
    Silver layer: hourly wind data per grid point.
    Deduplicates by (datetime, lat, lon), keeping the most recent extraction.
    One row per hour per grid point with wind speed, gusts, and direction.
#}

with base as (

    select *
    from {{ ref('stg_om_wind') }}

    {% if is_incremental() %}
    where _sdc_extracted_at > (
        select coalesce(max(_sdc_extracted_at), '1900-01-01'::timestamp)
        from {{ this }}
    )
    {% endif %}

),

deduped as (

    select distinct on (datetime, lat, lon)
        datetime,
        lat,
        lon,
        wind_speed_ms,
        wind_gusts_ms,
        wind_direction_deg,
        model,
        _sdc_extracted_at
    from base
    order by datetime, lat, lon, _sdc_extracted_at desc

)

select * from deduped
