{{ config(
    materialized         = 'incremental',
    unique_key           = ['datetime', 'lat', 'lon'],
    incremental_strategy = 'merge',
    indexes = [
        {'columns': ['datetime', 'lat', 'lon'], 'unique': true},
        {'columns': ['datetime']},
        {'columns': ['geoposition'], 'type': 'gist'},
    ]
) }}

{#
    Silver layer: 15-minute observations per grid point.
    Deduplicates by (datetime, lat, lon), keeping the most recent extraction.
    Timestamps are TIMESTAMPTZ (UTC-normalized from staging).
    Includes PostGIS geography point for spatial queries.
#}

with base as (

    select *
    from {{ ref('stg_om_obs') }}

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

        ST_SetSRID(
            ST_MakePoint(lon::double precision, lat::double precision),
            4326
        )::geography as geoposition,

        temp_c,
        apparent_temp_c,
        wind_speed_ms,
        wind_gusts_ms,
        wind_direction_deg,
        precip_mm,
        model,
        _sdc_extracted_at
    from base
    order by datetime, lat, lon, _sdc_extracted_at desc

)

select * from deduped
