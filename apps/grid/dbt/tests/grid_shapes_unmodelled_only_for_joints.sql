-- 'unmodelled' is the joint-only placeholder for a joint the thermal model did
-- not tier. Cable and substation rows leave thermal_tier NULL instead, so the
-- frontend can tell "no thermal model here" from "thermal model, no tier".

select
    segment_id,
    asset_type,
    thermal_tier
from {{ ref('grid_shapes') }}
where thermal_tier = 'unmodelled'
  and asset_type <> 'joint'
