{{ config(materialized='view') }}

select
    device_id,
    baseline_type,
    slot,
    is_weekday,
    baseline_kwh,
    computed_at
from {{ source('rec_flexibility_raw', '_rec_device_baselines_raw') }}
