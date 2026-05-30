{{ config(
    materialized='incremental',
    unique_key='id',
    schema='silver',
    indexes=[
        {'columns': ['geometry'], 'type': 'gist'}
    ]
) }}

with base as (
    select
        id,
        ST_GeomFromGeoJSON(geometry::text) as geometry,
        properties::jsonb as props,
        _sdc_extracted_at
    from {{ ref('stg_vincoli_indiretti') }}
)

select
    id,
    geometry,
    _sdc_extracted_at,
    props->>'denominazione' as denominazione,
    props->>'tipo_vincolo' as tipo_vincolo,
    ST_Area(geometry::geography) as area_m2,

    (
        jsonb_build_object(
            'type', 'Feature',
            'geometry', public.ST_AsGeoJSON(geometry)::jsonb,
            'properties', '{}'::jsonb
        )
    )::text as geom_geojson

from base

{% if is_incremental() %}
  where _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ this }})
{% endif %}
