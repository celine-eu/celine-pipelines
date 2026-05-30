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
    from {{ ref('stg_aree_non_idonee') }}
)

select
    id,
    geometry,
    _sdc_extracted_at,
    props->>'nome' as nome,
    props->>'tipo' as tipo,
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
