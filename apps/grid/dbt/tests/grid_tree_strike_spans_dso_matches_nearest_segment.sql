-- A span's operator comes from its silver stamp, never from geometry. The
-- nearest CIM segment of the same line within 50 m (the one the unit, feeder and
-- substation are taken from) must belong to that same operator; a mismatch means
-- an export stamped with the wrong operator, or two operators' lines sharing a
-- name at a border.
{% set seg   = source('grid_silver', 'silver_grid_ac_line_segment') %}

select sp.span_id, sp.dso_id, near.dso_id as nearest_segment_dso_id
from {{ ref('grid_tree_strike_spans') }} sp
cross join lateral (
    select s.dso_id
    from {{ seg }} s
    where s.line_name = sp.line_name
      and ST_DWithin(s.geom, sp.geom, 50)
    order by ST_Distance(s.geom, sp.geom) asc
    limit 1
) near
where near.dso_id is distinct from sp.dso_id
