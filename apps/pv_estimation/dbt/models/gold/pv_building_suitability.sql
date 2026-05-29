{{ config(
    materialized='incremental',
    unique_key='building_id',
    incremental_strategy='merge',
    schema='gold',
    post_hook=[
        "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_geometry ON {{ this }} USING GIST (geometry)"
    ]
) }}

with buildings as (
    select
        building_id,
        geometry,
        building_class,
        building_subtype,
        height,
        num_floors,
        footprint_area_m2,
        _sdc_extracted_at
    from {{ ref('pv_overture_buildings') }}
    {% if is_incremental() %}
    where _sdc_extracted_at > (
        select coalesce(max(_sdc_extracted_at), '1900-01-01'::timestamp)
        from {{ this }}
    )
    {% endif %}
),

suitable_hits as (
    select distinct b.building_id
    from buildings b
    join {{ ref('pv_aree_idonee') }} s on ST_Intersects(b.geometry, s.geometry)
),

unsuitable_hits as (
    select distinct b.building_id
    from buildings b
    join {{ ref('pv_aree_non_idonee') }} u on ST_Intersects(b.geometry, u.geometry)
),

direct_hits as (
    select distinct b.building_id
    from buildings b
    join {{ ref('pv_vincoli_diretti') }} cd on ST_Intersects(b.geometry, cd.geometry)
),

indirect_hits as (
    select distinct b.building_id
    from buildings b
    join {{ ref('pv_vincoli_indiretti') }} ci on ST_Intersects(b.geometry, ci.geometry)
)

select
    b.building_id,
    b.geometry,
    b.building_class,
    b.building_subtype,
    b.height,
    b.num_floors,
    b.footprint_area_m2,
    b._sdc_extracted_at,

    sh.building_id is not null as in_suitable_area,
    uh.building_id is not null as in_unsuitable_area,
    dh.building_id is not null as has_direct_constraint,
    ih.building_id is not null as has_indirect_constraint,

    (
        jsonb_build_object(
            'type', 'Feature',
            'geometry', public.ST_AsGeoJSON(b.geometry)::jsonb,
            'properties', '{}'::jsonb
        )
    )::text as geom_geojson

from buildings b
left join suitable_hits sh on sh.building_id = b.building_id
left join unsuitable_hits uh on uh.building_id = b.building_id
left join direct_hits dh on dh.building_id = b.building_id
left join indirect_hits ih on ih.building_id = b.building_id
