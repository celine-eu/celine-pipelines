{{ config(
    materialized='incremental',
    unique_key='osm_id',
    schema='silver'
) }}

select *
from {{ ref('openstreetmap_it_alpecimbra_base') }}
where piste_type is not null
   or aerialway is not null

{% if is_incremental() %}
  and _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ this }})
{% endif %}
