{{ config(
    materialized='view',
    schema='raw'
) }}

select
    geometry,
    features,
    metadata,
    _sdc_extracted_at,
    _sdc_received_at,
    _sdc_batched_at,
    _sdc_deleted_at,
    _sdc_sequence
from {{ source('raw', 'overture_buildings') }}
