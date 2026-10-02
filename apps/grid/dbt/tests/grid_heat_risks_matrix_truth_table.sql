-- Load-bearing test of the soil x thermal matrix on cable segments.
--
-- The matrix is restated here literally (not through the macro) so the test
-- fails if the macro itself drifts:
--   GREEN  -> NORMAL for every tier
--   ORANGE -> WARNING when thermal_tier = 'high', else NORMAL
--   RED    -> ALERT   when thermal_tier = 'high', else WARNING
--   NULL status (no weather match) -> NULL level
-- On a risk row thermal_tier is never NULL (unmodelled arcs read 'low').
--
-- Scoped to the current run window (date >= current_date, the first day the
-- model writes): history rows predate the thermal/soil columns and are NULL
-- there (on_schema_change = append_new_columns does not backfill).

select
    date,
    dso_id,
    line_name,
    municipality,
    heat_status,
    thermal_tier,
    thermal_modelled,
    risk_level,
    escalated_by_thermal
from {{ ref('grid_heat_risks') }}
where date >= current_date
  and (
   thermal_tier is null
   or risk_level is distinct from (
        case
            when heat_status is null                              then null
            when heat_status = 'RED'    and thermal_tier = 'high' then 'ALERT'
            when heat_status = 'RED'                              then 'WARNING'
            when heat_status = 'ORANGE' and thermal_tier = 'high' then 'WARNING'
            when heat_status = 'ORANGE'                           then 'NORMAL'
            when heat_status = 'GREEN'                            then 'NORMAL'
        end
   )
   or escalated_by_thermal is distinct from
        coalesce(thermal_tier = 'high' and heat_status in ('ORANGE', 'RED'), false)
  )
