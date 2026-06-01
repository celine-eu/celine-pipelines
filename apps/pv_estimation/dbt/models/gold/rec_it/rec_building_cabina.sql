{{ config(
    materialized='table',
    schema='gold',
    indexes=[
        {'columns': ['cod_ac']},
        {'columns': ['building_id']}
    ]
) }}

select
    b.building_id,
    c.cod_ac,
    c.rag_soc
from {{ source('overture_silver', 'pv_overture_buildings') }} b
join {{ source('rec_it_gold', 'gse_cabine_primarie') }} c
    on ST_Intersects(b.geometry, c.geometry)
