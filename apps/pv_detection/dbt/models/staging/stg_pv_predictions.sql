{{ config(
    materialized='view',
    schema='raw'
) }}

select
    building_id,
    has_pv,
    confidence,
    model_name,
    lon,
    lat,
    _sdc_extracted_at
from {{ source('raw', 'pv_predictions') }}
