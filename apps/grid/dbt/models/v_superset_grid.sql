{{ config(materialized='view', schema='gold') }}

{#
    Superset visualization view — today and future forecasts only.

    Joins risk data with shape geometry at query time so geometry is never
    replicated across daily risk rows. Uses the PostGIS geom column (compact
    binary) rather than the pre-serialized feature_geojson text blob.

    Superset deck.gl map layers should reference the geom column directly.
    The metrics JSONB column can be unpacked in Superset calculated columns
    for vector-specific fields.
#}

select
    r.date,
    r.risk_vector,
    r.risk_level,
    r.risk_color_hex,
    r.metrics,
    s.asset_type,
    s.asset_key,
    s.conductor_type,
    s.operational_unit,
    s.municipality,
    s.parent_substation_name,
    s.feeder_id,
    s.geom

from {{ ref('grid_risks') }} r
join {{ ref('grid_shapes') }} s using (segment_id)
where r.date >= current_date
