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
    from {{ ref('openstreetmap_fi_lappeenranta') }}
)

select
    osm_id,
    element,
    geometry,
    _sdc_extracted_at,
    tags->>'name'            as name,
    tags->>'amenity'         as amenity,
    tags->>'social_facility' as social_facility,
    tags->>'operator'        as operator,
    tags->>'addr:city'       as addr_city,
    tags->>'addr:street'     as addr_street,
    tags->>'addr:postcode'   as addr_postcode,
    tags->>'phone'           as phone,
    tags->>'website'         as website,
    tags->>'start_date'      as start_date,

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
