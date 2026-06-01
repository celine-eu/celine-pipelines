{{ config(
    materialized='incremental',
    unique_key='building_id',
    incremental_strategy='merge',
    schema='gold',
    indexes=[
        {'columns': ['geometry'], 'type': 'gist'}
    ]
) }}

with estimates as (
    select
        building_id,
        kwp,
        capex,
        annual_production_kwh,
        annual_consumption_kwh,
        user_type,
        regime,
        npv,
        irr,
        payback_simple,
        payback_discounted,
        tasso_autoconsumo,
        estimated_at
    from {{ ref('pv_roi_estimates') }}
    {% if is_incremental() %}
    where estimated_at > (
        select coalesce(max(estimated_at), '1900-01-01'::timestamp)
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
    e.building_id,
    b.geometry,
    b.building_class,
    b.building_subtype,
    b.height,
    b.num_floors,
    b.footprint_area_m2,
    e.kwp,
    e.capex,
    e.annual_production_kwh,
    e.annual_consumption_kwh,
    e.user_type,
    e.regime,
    e.npv,
    e.irr,
    e.payback_simple,
    e.payback_discounted,
    e.tasso_autoconsumo,
    e.estimated_at,

    (
        jsonb_build_object(
            'type', 'Feature',
            'geometry', public.ST_AsGeoJSON(b.geometry)::jsonb,
            'properties', '{}'::jsonb
        )
    )::text as geom_geojson

from estimates e
join buildings b on b.building_id = e.building_id
