{{ config(
    materialized         = 'incremental',
    unique_key           = ['date', 'lat', 'lon'],
    incremental_strategy = 'merge'
) }}

{#
    Silver layer: daily soil temperature per grid point.
    Deduplicates by (date, lat, lon), keeping the most recent extraction.
    Drops rows where soil_1m_c could not be computed (either source layer
    NULL, most commonly the most recent days of the archive lookback
    window before ERA5-Land has caught up).
#}

with base as (

    select *
    from {{ ref('stg_om_soil') }}
    where soil_1m_c is not null

    {% if is_incremental() %}
    and _sdc_extracted_at > (
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
        soil_28_100cm_c,
        soil_100_255cm_c,
        soil_1m_c,
        api_lat,
        api_lon,
        _sdc_extracted_at
    from base
    order by date, lat, lon, _sdc_extracted_at desc

)

select * from deduped
