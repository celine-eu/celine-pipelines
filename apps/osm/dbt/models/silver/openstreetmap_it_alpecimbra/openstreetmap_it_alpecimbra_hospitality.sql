{{ config(
    materialized='incremental',
    unique_key='osm_id',
    schema='silver'
) }}

select *
from {{ ref('openstreetmap_it_alpecimbra_base') }}
where tourism in ('hotel','guest_house','alpine_hut','resort','hostel','camp_site','caravan_site')

{% if is_incremental() %}
  and _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ this }})
{% endif %}
