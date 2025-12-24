{{ config(
    materialized = 'incremental',
    unique_key   = 'record_id'
) }}

select
    record_id,
    datetime_utc,
    latitude,
    longitude,
    level_type,
    level,
    variable_name,
    value,
    cast(null as integer)  as forecast_step,
    'an'                   as data_type,
    date_day,
    hour_utc
from {{ ref('copernicus_era5_single_levels') }}
where data_type = 'an'

{% if is_incremental() %}
  and datetime_utc > (
      select coalesce(max(datetime_utc), '1900-01-01')
      from {{ this }}
  )
{% endif %}