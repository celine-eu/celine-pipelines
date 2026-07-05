-- One row per (user_id, rec_id, sensor_id).
-- Unnesting sensor_ids here keeps gold models as pure aggregations
-- with no array handling or raw-source joins.
--
-- Substation assignment: topology_ids[1] is used when a participant
-- lists multiple cabine primarie. See rec_virtual_consumption_15m
-- for the full design rationale.
--
-- Materialized as a view (overriding the rec_it silver default of `table`):
-- this is a tiny pass-through of raw.rec_registry_mirror, and a stale table
-- snapshot caused the registry to get stuck at the original cohort while the
-- mirror grew — silently dropping newer participants and their devices from
-- every downstream gold model that joins on sensor_id. A view always reflects
-- the current mirror.
{{ config(materialized='view') }}

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
