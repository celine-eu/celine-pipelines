{{ config(
    materialized='incremental',
    unique_key='osm_id',
    schema='silver'
) }}

select *
from {{ ref('openstreetmap_fi_lappeenranta_base') }}
where amenity = 'social_facility'
   or social_facility is not null

{% if is_incremental() %}
  and _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ this }})
{% endif %}
