-- One row per (member, supply point), for every active member of every community.
--
-- **Consent is not applied here, and must not be.** The dataset is consent-gated:
-- a query arriving through a dataspace carries the members who consented to the
-- offer it runs under (registry-native principals, never DIDs), and the
-- `direct_user_match` row filter on `user_id` narrows the rows to them. Filtering
-- here as well would make a second, stale record of who consents.
--
-- The list a community hands its distributor is exactly this dataset read under
-- the offer naming that distributor.
--
-- A view, for the reason `silver_rec_registry` is one: the mirror is replaced
-- wholesale, and a table snapshot of it would silently keep an old cohort.
{{ config(materialized='view') }}

select
    user_id,
    rec_id,
    unnest(delivery_point_ids) as pod_code,
    last_updated
from {{ source('raw', 'rec_registry_mirror') }}
where cardinality(delivery_point_ids) > 0
