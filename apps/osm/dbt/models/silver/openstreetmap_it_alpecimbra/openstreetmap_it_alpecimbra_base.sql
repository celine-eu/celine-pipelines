{{ config(
    materialized='incremental',
    unique_key='osm_id',
    schema='silver'
) }}

with base as (
    select
        osm_id,
        element,
        ST_GeomFromText(geometry, 4326) as geometry,
        features::jsonb as tags,
        _sdc_extracted_at
    from {{ ref('openstreetmap_it_alpecimbra') }}
)

select
    osm_id,
    element,
    geometry,
    _sdc_extracted_at,
    tags->>'piste:type'   as piste_type,
    tags->>'aerialway'    as aerialway,
    tags->>'tourism'      as tourism,
    tags->>'amenity'      as amenity,
    tags->>'leisure'      as leisure,
    tags->>'heritage'     as heritage,
    tags->>'name'         as name,

    -- Superset-ready GeoJSON feature
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
