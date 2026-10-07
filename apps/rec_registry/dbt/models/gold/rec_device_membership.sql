-- Membership by device: one row per (community_id, device_id), for every meter sensor
-- of every active member of every community the registry exports.
--
-- The one place the other REC apps read membership from (rec_it, rec_flexibility).
-- The registry is the source of truth for it: which devices belong to which community,
-- with which role, and behind which primary substation. Which community a
-- *measurement* belongs to is the measurement's own (its upstream topic); a device the
-- registry lists under two communities appears once under each, and does not copy a
-- reading into the second.
--
-- No user_id: who the member is stays in the mirror.
--
-- substation_id is topology_ids[1], right only when an area lists exactly one node
-- equal to its boundary; tests/rec_registry_mirror_substation_is_area_boundary.sql fails
-- the build otherwise.
--
-- A view: the mirror is replaced on every run, and a table snapshot of it once kept an
-- old cohort while the mirror grew, silently dropping newer devices from every model
-- that joined on it.
{{ config(materialized='view') }}

select
    community_id,
    unnest(sensor_ids)  as device_id,
    role,
    member_type,
    -- CIM: Substation (cabina primaria)
    topology_ids[1]     as substation_id
from {{ source('raw', 'rec_registry_mirror') }}
where cardinality(sensor_ids) > 0
