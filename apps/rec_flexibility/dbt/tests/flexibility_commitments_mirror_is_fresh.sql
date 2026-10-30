-- raw.flexibility_commitments_mirror is replaced every 15 minutes by
-- apps/rec_flexibility_commitments; this app settles on it once a day. A mirror that
-- stopped refreshing settles stale statuses as current. Its run marker in
-- raw.mirror_refresh older than 2 hours (eight missed runs) fails this run; so does a
-- missing marker. The marker, not max(last_updated): an export with no commitment in
-- the 90-day window empties the mirror and still moves the marker.
select max(refreshed_at) as last_refreshed, now() - max(refreshed_at) as age
from {{ source('raw_mirror_refresh', 'mirror_refresh') }}
where mirror = 'flexibility_commitments_mirror'
having max(refreshed_at) is null
    or max(refreshed_at) < now() - interval '2 hours'
