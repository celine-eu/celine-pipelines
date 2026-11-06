{{ config(materialized='view', tags=['rec_flexibility', 'fair']) }}

-- Fair gamification (spec 2026-10-07, design 2026-10-09 §4.2/§4.7): every POD with M1
-- data in the last 120 days, with its registry category and the manual overrides.
--
-- category comes from the registry member_type (never from role, wrong for several PODs):
--   schema:Person → household, schema:GovernmentOrganization / schema:Organization →
--   public_entity, schema:LocalBusiness → business (reference group only). A device with
--   M1 data but no membership row is kept (is_in_app = false, category 'unknown') so it
--   can still serve as a peer once an override gives it a category. Its community is the
--   one most members belong to (or fair_default_community_id when set).
-- The PV class is NOT decided here (data-driven, per week, in rec_fair_device_class);
-- only the override and the meter kind (CF2/CF4 carry an export register) pass through.

with m1_devices as (
    select device_id, max(cf_type) as cf_type
    from {{ source('metering_silver', 'meters_data') }}
    where meter_type = 'M1'
      and ts >= now() - interval '120 days'
    group by device_id
),
members as (
    select distinct on (device_id)
        device_id, community_id, role, member_type, substation_id
    from {{ source('rec_registry_gold', 'rec_device_membership') }}
    order by device_id, community_id
),
ov as (
    select device_id, pv_class_override, category_override, quarantine_from, quarantine_reason, battery_declared
    from {{ ref('rec_device_overrides') }}
),
-- Devices with M1 data but no membership row join the community most members belong to,
-- unless fair_default_community_id names one explicitly.
default_community as (
    select coalesce(
        nullif('{{ var("fair_default_community_id") }}', ''),
        (select community_id from members group by community_id order by count(*) desc, community_id limit 1),
        'unknown'
    ) as community_id
)
select
    d.device_id,
    coalesce(m.community_id, dc.community_id)                          as community_id,
    (m.device_id is not null)                                           as is_in_app,
    m.role,
    m.member_type,
    m.substation_id,
    d.cf_type,
    (d.cf_type in ('CF2', 'CF4'))                                       as has_export_register,
    coalesce(
        ov.category_override,
        case m.member_type
            when 'schema:Person'                 then 'household'
            when 'schema:GovernmentOrganization' then 'public_entity'
            when 'schema:Organization'           then 'public_entity'
            when 'schema:LocalBusiness'          then 'business'
            else 'unknown'
        end
    )                                                                   as category,
    case
        when ov.category_override is not null then 'override'
        when m.member_type is not null        then 'registry'
        else 'none'
    end                                                                 as category_source,
    ov.pv_class_override,
    ov.quarantine_from,
    ov.quarantine_reason,
    coalesce(ov.battery_declared, false)                                as battery_declared
from m1_devices d
cross join default_community dc
left join members m using (device_id)
left join ov using (device_id)
