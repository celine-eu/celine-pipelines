{{ config(
    materialized='view',
    schema='raw'
) }}

select
    id,
    type,
    geometry,
    geometry_name,
    properties,
    _sdc_extracted_at,
    _sdc_received_at,
    _sdc_batched_at,
    _sdc_deleted_at,
    _sdc_sequence
from {{ source('raw', 'aree_non_idonee') }}
