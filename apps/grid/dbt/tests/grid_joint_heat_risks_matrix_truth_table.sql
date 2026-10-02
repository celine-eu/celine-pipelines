-- The same matrix, restated literally, on the thermal joints.
-- Joints only enter the model when joint_tier is known, so thermal_tier is
-- never NULL and never 'unmodelled' here.

select
    date,
    dso_id,
    joint_id,
    heat_status,
    thermal_tier,
    risk_level,
    escalated_by_thermal
from {{ ref('grid_joint_heat_risks') }}
where thermal_tier is null
   or thermal_tier = 'unmodelled'
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
