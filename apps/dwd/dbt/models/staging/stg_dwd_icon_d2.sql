{{ config(
    materialized = 'view',
    schema = 'staging'
) }}

select distinct on (datetime, lat, lon, name)
    datetime,
    base_datetime,
    lat,
    lon,
    level_type,
    level,
    name,
    value,
    forecast_step,
    forecast_time,
    forecast_time_units,
    centre,
    data_type,
    grid_type,
    _sdc_filename,
    _sdc_last_modified
from {{ source('raw', 'dwd_icon_d2') }}
where level_type = 'heightAboveGround'
  and level = 10
  and name in ('10u', '10v', 'max_i10fg')
  and datetime >= current_date
order by
    datetime, lat, lon, name,
    base_datetime desc,
    _sdc_last_modified desc
