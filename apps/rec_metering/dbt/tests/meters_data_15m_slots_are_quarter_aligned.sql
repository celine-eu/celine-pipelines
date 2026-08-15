-- Every reading must sit on a 15-minute boundary.
--
-- `ts` is the slot start, and downstream models join on it as if it were one:
-- rec_flexibility buckets by it, meters_data_1h groups it into hours, and
-- meters_data_15m_missing_intervals generates the expected grid from
-- generate_series(..., interval '15 minutes'). An off-grid timestamp does not
-- fail any of those — it silently becomes a slot of its own that the expected
-- grid never contains, so it is invisible in the gap report and double-counts
-- against the aligned reading for the same quarter hour.
--
-- Evaluated at UTC rather than in the session time zone: `ts` is a timestamptz,
-- and `extract(minute from ...)` on one is resolved against whatever TimeZone
-- the connection happens to carry.

select
    _id,
    device_id,
    ts
from {{ ref('meters_data_15m') }}
where extract(minute from ts at time zone 'UTC')::int % 15 <> 0
   or extract(second from ts at time zone 'UTC') <> 0
