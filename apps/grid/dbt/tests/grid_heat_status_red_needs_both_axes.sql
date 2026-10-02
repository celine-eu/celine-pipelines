-- Truth table of the two-axis heat status, restated independently of the model.
--
-- RED needs BOTH axes hot: the soil axis at ORANGE and the air axis at RED.
-- ORANGE is the soil axis alone. Everything else is GREEN.
-- A row that disagrees is returned and fails the test.

select
    date,
    lat,
    lon,
    soil_status,
    air_heat_tier,
    heat_status
from {{ ref('grid_heat_status') }}
where heat_status is distinct from (
    case
        when soil_status = 'ORANGE' and air_heat_tier = 'RED' then 'RED'
        when soil_status = 'ORANGE'                           then 'ORANGE'
        else 'GREEN'
    end
)
