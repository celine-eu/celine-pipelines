-- soil_status and soil7_mean_c must be populated only when the trailing
-- 7-day window is complete (soil_data_complete = true), and NULL
-- otherwise. Any row where the two disagree fails this test.
select date, lat, lon, soil_data_complete, soil_status, soil7_mean_c
from {{ ref('om_soil_heat_risk') }}
where (soil_data_complete and soil_status is null)
   or (not soil_data_complete and soil_status is not null)
   or (soil_data_complete and soil7_mean_c is null)
   or (not soil_data_complete and soil7_mean_c is not null)
