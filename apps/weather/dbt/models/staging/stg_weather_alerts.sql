{{- config(materialized='view') -}}

{#
    Alerts with location assignment.
    Contract table alerts may be regional (location_id is null for
    area-based providers like MT). Cross-join those with seed locations
    matching the provider. Per-location alerts pass through directly.
#}

with locations as (
    select * from {{ ref('weather_locations') }}
),

regional as (
    select
        loc.location_id,
        src.provider,
        src.alert_id,
        src.event,
        src.severity,
        src.urgency,
        src.headline,
        src.description,
        src.starts_at,
        src.expires_at,
        src.source_name,
        src.area_desc
    from {{ source('weather_contract', 'weather__alerts') }} src
    cross join locations loc
    where src.location_id is null
      and loc.provider = src.provider
),

located as (
    select
        src.location_id,
        src.provider,
        src.alert_id,
        src.event,
        src.severity,
        src.urgency,
        src.headline,
        src.description,
        src.starts_at,
        src.expires_at,
        src.source_name,
        src.area_desc
    from {{ source('weather_contract', 'weather__alerts') }} src
    where src.location_id is not null
)

select * from regional
union all
select * from located
