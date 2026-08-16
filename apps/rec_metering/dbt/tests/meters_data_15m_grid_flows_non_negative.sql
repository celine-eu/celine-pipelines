-- Grid import and grid export are magnitudes, so neither can be negative.
--
-- A negative value here is a sign error upstream, and it is not self-evident
-- downstream: rec_it nets import against export per substation with least(),
-- and rec_flexibility sums both into settlement. A negative import silently
-- inflates the community's virtual self-consumption instead of failing.
--
-- `self_consumed_kwh` is deliberately NOT tested. It is published unclipped and
-- is negative on a minority of rows because the upstream computes it without a
-- max(...,0) clamp. That is a known upstream defect; asserting against it here
-- would turn a documented data-quality fact into a permanently red test.

select
    _id,
    device_id,
    ts,
    consumption_kwh,
    production_kwh
from {{ ref('meters_data_15m') }}
where consumption_kwh < 0
   or production_kwh < 0
