{{ config(
    materialized='incremental',
    unique_key='osm_id',
    schema='silver'
) }}

select *
from {{ ref('openstreetmap_it_alpecimbra_base') }}
where amenity in ('parking','parking_space')

{% if is_incremental() %}
  and _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ this }})
{% endif %}
