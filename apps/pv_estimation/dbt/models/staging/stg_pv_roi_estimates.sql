{{ config(
    materialized='view',
    schema='raw'
) }}

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
    _sdc_extracted_at
from {{ source('raw', 'pv_roi_estimates') }}
