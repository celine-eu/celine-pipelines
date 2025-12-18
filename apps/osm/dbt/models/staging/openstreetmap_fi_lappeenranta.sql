{{ config(
    materialized='view',
    schema='raw'
) }}

select
    element,
    id as osm_id,
    geometry,
    features,
    metadata,
    _sdc_extracted_at,
    _sdc_received_at,
    _sdc_batched_at,
    _sdc_deleted_at,
    _sdc_sequence
from {{ source('raw', 'openstreetmap_fi_lappeenranta') }}
