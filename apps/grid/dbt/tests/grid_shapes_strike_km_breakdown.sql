-- The tree-strike km breakdown of a tratta must be consistent with its worst
-- tier and never exceed the tratta length: the popup shows both side by side,
-- so a 'high' tratta with 0 km high, or km summing past the union length,
-- would be a visible contradiction on the map.
select segment_id, strike_tree_tier, strike_km_high, strike_km_mid, strike_km_low, length_m
from {{ ref('grid_shapes') }}
where asset_type = 'ac_line_segment'
  and (
        coalesce(strike_km_high, 0) + coalesce(strike_km_mid, 0) + coalesce(strike_km_low, 0)
            > length_m / 1000.0 + 0.001
     or (strike_tree_tier = 'high' and coalesce(strike_km_high, 0) <= 0)
     or (strike_tree_tier = 'mid'  and (coalesce(strike_km_mid, 0) <= 0 or coalesce(strike_km_high, 0) > 0))
     or (strike_tree_tier = 'low'  and (coalesce(strike_km_low, 0) <= 0 or coalesce(strike_km_high, 0) + coalesce(strike_km_mid, 0) > 0))
     or (strike_tree_tier is null
         and coalesce(strike_km_high, 0) + coalesce(strike_km_mid, 0) + coalesce(strike_km_low, 0) > 0)
  )
union all
-- density and tier share the coverage rule: both NULL or both set
select segment_id, strike_tree_tier, strike_km_high, strike_km_mid, strike_km_low, length_m
from {{ ref('grid_shapes') }}
where asset_type = 'ac_line_segment'
  and (strike_tree_tier is null) <> (strike_density_per_km is null)
