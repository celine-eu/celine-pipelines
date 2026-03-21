{{ config(
    materialized         = 'incremental',
    unique_key           = ['date', 'lat', 'lon'],
    incremental_strategy = 'merge'
) }}

{#
    Silver layer: daily max temperature per grid point.
    Deduplicates by (date, lat, lon), keeping the most recent extraction.
    Adds altitude_band classification based on API-returned elevation.
#}

with base as (

    select *
    from {{ ref('stg_om_heat') }}

    {% if is_incremental() %}
    where _sdc_extracted_at > (
        select coalesce(max(_sdc_extracted_at), '1900-01-01'::timestamp)
        from {{ this }}
    )
    {% endif %}

),

deduped as (

    select distinct on (date, lat, lon)
        date,
        lat,
        lon,
        temp_max_c,
        elevation_m,
        case
            when elevation_m < 500  then 'low'
            when elevation_m < 1000 then 'mid'
            else 'high'
        end as altitude_band,
        model,
        _sdc_extracted_at
    from base
    order by date, lat, lon, _sdc_extracted_at desc

)

select * from deduped
