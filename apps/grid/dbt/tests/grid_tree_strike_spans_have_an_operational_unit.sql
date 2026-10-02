{{ config(severity='warn') }}
-- Every span should find a CIM segment of its line within 50 m to inherit the
-- operational unit from; a miss is a geometry/CRS drift worth a look, not a failure.
select span_id, line_name, municipality
from {{ ref('grid_tree_strike_spans') }}
where operational_unit is null
