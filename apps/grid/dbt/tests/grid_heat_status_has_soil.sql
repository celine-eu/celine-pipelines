{{ config(severity='error') }}
-- Every heat point should find a complete soil row within 10 km and 10 days.
-- A miss means the soil axis silently reads GREEN for that point, which maps
-- to NORMAL for every tier under the matrix: this switches the whole heat
-- vector off for that point, not just lowers it. Promoted from warn to error
-- (0.13.0 final-review fix wave) because GREEN-by-default makes soil coverage
-- load-bearing, not merely informative.

select
    date,
    lat,
    lon
from {{ ref('grid_heat_status') }}
where soil_asof_date is null
