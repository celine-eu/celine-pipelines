-- Every row in rec_flexibility_bonus must correspond to an active commitment
-- (status in ('committed','settled')) that covers the window. Failing rows mean the
-- commitment-gating semi-join was bypassed upstream.
select b.device_id, b.window_start
from {{ ref('rec_flexibility_bonus') }} b
left join {{ ref('silver_flexibility_commitments') }} c
       on c.device_id    = b.device_id
      and b.window_start >= c.period_start
      and b.window_end   <= c.period_end
      and c.status in ('committed', 'settled')
where c.device_id is null
