{{
  config(
    unique_key='cod_ac',
    incremental_strategy='merge',
    merge_update_columns=[
      'geometry',
      'rag_soc',
      'cod_conseg',
      'shape__area',
      'shape__length',
      'last_updated_at'
    ],
    indexes=[
      {'columns': ['geometry'], 'type': 'gist'},
      {'columns': ['cod_ac']}
    ]
  )
}}

select
    ST_GeomFromText(geometry, 4326)::geometry as geometry,
    rag_soc,
    cod_ac,
    cod_conseg,
    shape__area,
    shape__length,
    last_updated_at
from {{ ref('silver_gse_cabine_primarie') }}

{% if is_incremental() %}
where last_updated_at > (
    select coalesce(max(last_updated_at), '1900-01-01'::timestamp)
    from {{ this }}
)
{% endif %}
