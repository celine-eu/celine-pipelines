{{ config(
    materialized='incremental',
    unique_key='building_id',
    incremental_strategy='merge',
    schema='gold',
    indexes=[
        {'columns': ['geometry'], 'type': 'gist'},
        {'columns': ['has_pv']}
    ]
) }}

with detections as (
    select
        building_id,
        has_pv,
        confidence,
        model_name,
        detected_at
    from {{ ref('pv_detections') }}
    {% if is_incremental() %}
    where detected_at > (
        select coalesce(max(detected_at), '1900-01-01'::timestamp)
        from {{ this }}
    )
    {% endif %}
),

buildings as (
    select
        building_id,
        geometry,
        building_class,
        building_subtype,
        height,
        num_floors,
        footprint_area_m2
    from {{ source('overture_silver', 'pv_overture_buildings') }}
)

select
    d.building_id,
    b.geometry,
    d.has_pv,
    d.confidence,
    d.model_name,
    d.detected_at,
    b.building_class,
    b.building_subtype,
    b.height,
    b.num_floors,
    b.footprint_area_m2

from detections d
join buildings b on b.building_id = d.building_id
