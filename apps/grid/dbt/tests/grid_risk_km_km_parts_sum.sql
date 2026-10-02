-- km_alert + km_warning + km_normal must equal km_total for every row.
select *
from {{ ref('grid_risk_km') }}
where abs(km_alert + km_warning + km_normal - km_total) > 1e-6
