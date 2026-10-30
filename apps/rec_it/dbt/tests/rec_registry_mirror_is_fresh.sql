-- raw.rec_registry_mirror is replaced every 5 minutes by apps/rec_registry. A flow that
-- stops (failed, paused, the registry unreachable) leaves the last membership in place and
-- rec_device_membership keeps serving it silently. Older than 30 minutes (six missed runs)
-- fails this run; so does an empty mirror. A singular test because a flow cannot run
-- `dbt source freshness` (celine-utils DBT_VERBS).
{{ config(tags=['rec_it']) }}

select max(last_updated) as last_refreshed, now() - max(last_updated) as age
from {{ source('raw', 'rec_registry_mirror') }}
having max(last_updated) is null
    or max(last_updated) < now() - interval '30 minutes'
