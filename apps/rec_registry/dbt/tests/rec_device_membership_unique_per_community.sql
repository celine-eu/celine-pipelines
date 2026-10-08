-- At most one row per (community_id, device_id).
--
-- A sensor listed by two members of the same community would give two rows, and every
-- join on membership (rec_it, the rec_flexibility fleet) would count its readings twice.
-- There is no deduplication rule: this test fails the rec_registry run, and the listing
-- is corrected in the registry.
--
-- One failing row per duplicated pair, with how many members list it.
select community_id, device_id, count(*) as listed
from {{ ref('rec_device_membership') }}
group by community_id, device_id
having count(*) > 1
