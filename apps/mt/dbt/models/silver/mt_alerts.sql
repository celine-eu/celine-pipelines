{{ config(materialized='table') }}

{# Full replace — source is full-refresh per run. Normalize types only. #}
select
    identifier,
    sender,
    (cast(sent    as timestamp) at time zone 'Europe/Rome') as sent,
    (cast(expires as timestamp) at time zone 'Europe/Rome') as expires,
    source,
    status,
    language,
    headline,
    category,
    event,
    urgency,
    severity,
    certainty,
    description,
    web,
    contact,
    area_desc,
    area_polygon,
    resource_uri,
    msg_type,
    scope
from {{ ref('stg_mt_alerts') }}
