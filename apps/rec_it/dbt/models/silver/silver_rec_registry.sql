-- One row per (user_id, rec_id, sensor_id).
-- Unnesting sensor_ids here keeps gold models as pure aggregations
-- with no array handling or raw-source joins.
--
-- Substation assignment: topology_ids[1] is used when a participant
-- lists multiple cabine primarie. See rec_virtual_consumption_15m
-- for the full design rationale.

select
    user_id,
    rec_id,
    area,
    role,
    member_type,
    -- CIM: Substation (cabina primaria)
    topology_ids[1]         as substation_id,
    unnest(sensor_ids)      as sensor_id,
    last_updated
from {{ source('raw', 'rec_registry_mirror') }}
where cardinality(sensor_ids) > 0
