-- On heat rows, km_escalated is the thermal escalation (tier high under an
-- ORANGE or RED status), and km_thermal_high / km_thermal_mid are the static
-- thermal exposure of the tratta. Wind rows carry no thermal tier, so both
-- thermal km must be 0 there.

with expected as (

    select
        date,
        dso_id,
        operational_unit,
        line_name,
        municipality,
        conductor_type,
        coalesce(sum(length_m) filter (
            where thermal_tier = 'high' and heat_status in ('ORANGE', 'RED')
        ), 0) / 1000.0 as km_escalated,
        coalesce(sum(length_m) filter (where thermal_tier = 'high'), 0) / 1000.0 as km_thermal_high,
        coalesce(sum(length_m) filter (where thermal_tier = 'mid'),  0) / 1000.0 as km_thermal_mid
    from {{ ref('grid_heat_risks') }}
    where date is not null
    group by 1, 2, 3, 4, 5, 6

),

heat_rows as (

    select k.*, e.km_escalated as exp_escalated,
           e.km_thermal_high as exp_high,
           e.km_thermal_mid  as exp_mid
    from {{ ref('grid_risk_km') }} k
    join expected e
      on  k.date             =                  e.date
      and k.dso_id           =                  e.dso_id
      and k.line_name        =                  e.line_name
      and k.conductor_type   =                  e.conductor_type
      and k.municipality     is not distinct from e.municipality
      and k.operational_unit is not distinct from e.operational_unit
    where k.risk_vector = 'heat'

)

select date, risk_vector, line_name, municipality, conductor_type,
       km_escalated, exp_escalated, km_thermal_high, exp_high, km_thermal_mid, exp_mid
from heat_rows
where abs(km_escalated    - exp_escalated) > 1e-6
   or abs(km_thermal_high - exp_high)      > 1e-6
   or abs(km_thermal_mid  - exp_mid)       > 1e-6

union all

select date, risk_vector, line_name, municipality, conductor_type,
       km_escalated, null, km_thermal_high, null, km_thermal_mid, null
from {{ ref('grid_risk_km') }}
where risk_vector = 'wind'
  and (km_thermal_high <> 0 or km_thermal_mid <> 0)
