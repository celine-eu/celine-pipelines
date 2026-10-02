{{ config(severity = 'warn') }}

-- Warn (does not fail the build) when the latest soil data available is
-- more than 3 days stale, beyond the expected archive lag.
select max(date) as latest_date
from {{ ref('om_soil_heat_risk') }}
having max(date) < current_date - interval '3 days'
