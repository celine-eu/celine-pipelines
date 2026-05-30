{{ config(
    materialized='incremental',
    unique_key='building_id',
    schema='silver',
    indexes=[
        {'columns': ['geometry'], 'type': 'gist'}
    ]
) }}

with base as (
    select
        features::jsonb as props,
        ST_GeomFromText(geometry, 4326) as geometry,
        _sdc_extracted_at
    from {{ ref('stg_overture_buildings') }}
)

select
    md5(ST_AsText(geometry)) as building_id,
    geometry,
    _sdc_extracted_at,
    props->>'class' as building_class,
    props->>'subtype' as building_subtype,
    (props->>'height')::float as height,
    (props->>'num_floors')::int as num_floors,
    ST_Area(geometry::geography) as footprint_area_m2,

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
