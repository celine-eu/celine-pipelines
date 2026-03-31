{{ config(materialized='table') }}

{#
    Deduplicated active weather alerts for Trentino.
    One row per identifier, keeping the most recently sent version.
    Filters to alerts that have not yet expired (or have no expiry).
#}

select distinct on (identifier)
    identifier,
    sent,
    expires,
    event,
    severity,
    urgency,
    certainty,
    headline,
    description,
    area_desc,
    web,
    source
from {{ ref('mt_alerts') }}
where expires is null or expires > now()
order by identifier, sent desc
