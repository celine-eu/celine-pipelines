{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'record_id'
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
    forecast_step,
    'fc'              as data_type,
    date_day,
    hour_utc
from {{ ref('copernicus_era5_single_levels') }}
where data_type = 'fc'
  and datetime_utc >= '{{ run_started_at }}'::timestamptz
  