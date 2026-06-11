select
    fid,
    geometry,
    (features ->> 'cod_ac')::text                        as cod_ac,
    (features ->> 'rag_soc')::text                       as rag_soc,
    nullif(trim(features ->> 'cod_conseg'), '')::text     as cod_conseg,
    (features ->> 'shape__area')::double precision        as shape__area,
    (features ->> 'shape__length')::double precision      as shape__length,
    (features ->> 'objectid')::integer                    as objectid,
    (features ->> 'p_cab_pri')::text                      as p_cab_pri,
    (features ->> 't_cab_pri')::text                      as t_cab_pri,
    nullif(trim(features ->> 'txt1'), '')::text           as txt1,
    nullif(trim(features ->> 'txt2'), '')::text           as txt2,
    coalesce(
        _sdc_extracted_at,
        _sdc_received_at,
        _sdc_batched_at
    )                                                      as last_updated_at,
    _sdc_filename,
    _sdc_deleted_at
from {{ source('raw', 'gse_cabine_primarie') }}
where _sdc_deleted_at is null
