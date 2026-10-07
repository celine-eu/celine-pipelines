-- One row per (community_id, _id) in rec_flexibility_windows_community, its merge key.
--
-- _id hashes only the day and the window bounds, so it repeats when two communities have
-- a surplus window with the same bounds; a check on _id alone would fail then, and a
-- merge on _id alone would let the second community's window overwrite the first's.
select community_id, _id, count(*) as rows
from {{ ref('rec_flexibility_windows_community') }}
group by community_id, _id
having count(*) > 1
