{{ config(
    materialized='incremental',
    unique_key='building_id',
    incremental_strategy='merge',
    schema='silver'
) }}

with base as (
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
        _sdc_extracted_at,
        row_number() over (
            partition by building_id
            order by _sdc_extracted_at desc
        ) as rn
    from {{ ref('stg_pv_roi_estimates') }}
    {% if is_incremental() %}
    where _sdc_extracted_at > (
        select coalesce(max(estimated_at), '1900-01-01'::timestamp)
        from {{ this }}
    )
    {% endif %}
)

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
    _sdc_extracted_at as estimated_at
from base
where rn = 1
