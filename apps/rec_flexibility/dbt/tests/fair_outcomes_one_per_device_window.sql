-- Fair gamification: a matched community window yields at most one outcome per device
-- (the earliest committed commitment keeps it). Returns the offending (device, window) pairs.
select device_id, window_start, count(*) as n
from {{ ref('rec_fair_commitment_outcomes') }}
where matched
group by device_id, window_start
having count(*) > 1
