{{ config(
    materialized='incremental',
    unique_key='osm_id',
    schema='silver'
) }}

select *
from {{ ref('openstreetmap_it_alpecimbra_base') }}
where tourism in ('museum','artwork','gallery','attraction','exhibit')
   or heritage is not null
   or amenity in ('theatre','arts_centre')

{% if is_incremental() %}
  and _sdc_extracted_at > (select max(_sdc_extracted_at) from {{ this }})
{% endif %}
