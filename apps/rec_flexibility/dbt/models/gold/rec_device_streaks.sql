{{ config(materialized='view') }}

select
    device_id,
    level,
    peak,
    multiplier,
    computed_at
from {{ source('rec_flexibility_raw', '_rec_device_streaks_raw') }}
