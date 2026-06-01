{{ config(
    materialized='incremental',
    unique_key='building_id',
    incremental_strategy='merge',
    schema='silver'
) }}

with base as (
    select
        building_id,
        cast(has_pv as boolean) as has_pv,
        cast(confidence as float) as confidence,
        model_name,
        lon,
        lat,
        _sdc_extracted_at,
        row_number() over (
            partition by building_id
            order by _sdc_extracted_at desc
        ) as rn
    from {{ ref('stg_pv_predictions') }}
    {% if is_incremental() %}
    where _sdc_extracted_at > (
        select coalesce(max(detected_at), '1900-01-01'::timestamp)
        from {{ this }}
    )
    {% endif %}
)

select
    building_id,
    has_pv,
    confidence,
    model_name,
    lon,
    lat,
    _sdc_extracted_at as detected_at
from base
where rn = 1
