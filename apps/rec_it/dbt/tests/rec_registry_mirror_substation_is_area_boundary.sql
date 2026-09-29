-- Every member's substation_id must be the id of its area's boundary.
--
-- silver_rec_registry takes topology_ids[1] as the member's substation_id
-- (cabina primaria), and every rec_virtual_consumption_* model nets shared
-- energy per substation_id. That is right only when the member's area lists
-- exactly one topology node and that node's id is the cod_ac of the area's GSE
-- primary-substation boundary (boundary_id, mirrored by apps/rec_registry).
-- An area listing several nodes puts all its members under the first; a node
-- differing from the boundary puts them under a substation their supply points
-- are not in. Neither fails anything downstream: the energy is silently netted
-- in the wrong place.
--
-- Failing rows are areas, not members: one row per (rec_id, area) with how many
-- active member rows it holds.
--
-- Rows with a null boundary_id (areas exported before registry schema v0.7) are
-- not checked. A mirror table that predates the boundary_id column (the
-- rec_registry flow adds it on its next run) returns no rows rather than
-- erroring, so this test can ship before the mirror flow is redeployed.
{{ config(tags=['rec_it']) }}

{%- set has_boundary = false -%}
{%- if execute -%}
    {%- set cols = adapter.get_columns_in_relation(source('raw', 'rec_registry_mirror')) -%}
    {%- set has_boundary = 'boundary_id' in (cols | map(attribute='name') | map('lower') | list) -%}
{%- endif %}

{% if has_boundary %}
select
    rec_id,
    area,
    boundary_id,
    topology_ids,
    topology_ids[1]  as substation_id,
    count(*)         as member_rows
from {{ source('raw', 'rec_registry_mirror') }}
where boundary_id is not null
  and (
        cardinality(topology_ids) <> 1
     or topology_ids[1] is distinct from boundary_id
  )
group by rec_id, area, boundary_id, topology_ids
{% else %}
select
    null::text    as rec_id,
    null::text    as area,
    null::text    as boundary_id,
    null::text[]  as topology_ids,
    null::text    as substation_id,
    null::bigint  as member_rows
where false
{% endif %}
