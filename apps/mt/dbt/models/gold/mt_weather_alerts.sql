{{ config(
    materialized = 'table',
    alias        = 'weather__alerts'
) }}

{#
    Weather contract: active alerts normalized to the shared schema
    consumed by the weather pipeline.

    MT alerts are regional (area_desc) not per-location, so location_id
    is left null — the weather pipeline assigns them to all MT locations.

    Contract columns:
      provider, location_id, alert_id, event, severity, urgency,
      headline, description, starts_at, expires_at, source_name, area_desc
#}

select
    'mt'::text              as provider,
    null::text              as location_id,
    identifier              as alert_id,
    event,
    severity,
    urgency,
    headline,
    description,
    sent                    as starts_at,
    expires                 as expires_at,
    source                  as source_name,
    area_desc
from {{ ref('mt_alerts') }}
where expires is null or expires > now()
