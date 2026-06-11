-- One row per commitment. Normalises types from the raw mirror and adds
-- derived columns useful for downstream gold models.
-- No incremental — the raw mirror is already a 90-day sliding window,
-- so a full table rebuild on each run is inexpensive and simpler.

select
    id                                                          as commitment_id,
    user_id,
    suggestion_id,
    suggestion_type,
    community_id,
    -- device_id in flexibility-api = sensor_id in the metering pipeline
    device_id,
    period_start::timestamptz                                   as period_start,
    period_end::timestamptz                                     as period_end,
    committed_at::timestamptz                                   as committed_at,
    settled_at::timestamptz                                     as settled_at,
    reminded_at::timestamptz                                    as reminded_at,
    -- Strip any "StatusSchema." enum-repr prefix so downstream filters match bare tokens.
    split_part(status, '.', -1)                                 as status,
    reward_points_estimated,
    reward_points_actual,
    -- Convenience: duration of the commitment window in hours
    extract(epoch from period_end::timestamptz - period_start::timestamptz) / 3600
                                                                as window_duration_hours,
    last_updated
from {{ source('raw', 'flexibility_commitments_mirror') }}
