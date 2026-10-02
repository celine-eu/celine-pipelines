-- risk_index is a percentage-like score in [0, 100]; NULL only when km_total is 0.
select *
from {{ ref('grid_risk_km') }}
where risk_index < 0
   or risk_index > 100
   or (risk_index is null and km_total > 0)
