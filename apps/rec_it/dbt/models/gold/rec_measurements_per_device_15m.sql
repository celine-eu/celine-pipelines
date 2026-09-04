{{ config(materialized='view') }}

{#
    rec_virtual_consumption_per_device_15m, one quantity per row.

    Two published quantities per source row — the member's metered grid import
    and their pro-rata share of the community's shared energy. `ratio` is
    deliberately not published as an observation; see below.

    ── Governance: this one carries real row-level access control ──────────────
    The source model is filtered by the `rec_registry` handler on `device_id`, so
    a caller sees only their own devices' rows. **`device_id` is therefore carried
    verbatim and must never be dropped, renamed or replaced by `feature_iri`.**
    Nothing validates that a governance file's `row_filters[].args.column` exists
    on the table it names; a view that lost the column would be served with no
    filter at all rather than failing.

    The governance entry for this view shares the source's block by YAML anchor,
    so `access_level`, `ownership` and `row_filters` are one object in the file
    and cannot drift apart.

    ── The entity IRIs are relative, deliberately ──────────────────────────────
    `feature_iri` and `sensor_iri` are emitted as `connection-point/<device_id>`
    and `device/<device_id>`, with no base. They identify things *this
    deployment's data is about*, so the base is the serving deployment's —
    `dataset-api`'s `entity_base_uri`, which `MappingEngine._absolutize`
    prepends to any value not already starting `http`. This view hardcoded
    `https://w3id.org/celine-eu/id` until 2026-09-04 (issue #5).
    `observed_property` and `unit` stay absolute: they name a CELINE term and a
    QUDT unit, published by someone else and not a deployment's to rename.

    ── `ratio` is not an observation ───────────────────────────────────────────
    It is this row's share of the community denominator — a weight used to
    compute virtual_consumption_kwh, not a property of anything observed. There
    is no observable property for it and inventing one would put a computation's
    intermediate value into the catalogue as though it were a measurement. It
    stays in the source table, which is where a consumer recomputing the
    allocation would look.
#}

{% set celine_ns = 'https://w3id.org/celine-eu#' %}
{% set unit_kwh  = 'http://qudt.org/vocab/unit/KiloW-HR' %}

with base as (
    select
        _id,
        ts,
        device_id,
        rec_id,
        substation_id,
        consumption_kwh,
        virtual_consumption_kwh
    from {{ ref('rec_virtual_consumption_per_device_15m') }}
),

keyed as (
    select
        *,
        'connection-point/' || device_id as feature_iri
    from base
),

unpivoted as (

    {#
        Metered, so it has a sensor: the member's own meter.
    #}
    select
        md5(_id || '|GridImportEnergy')         as observation_id,
        ts                                      as result_time,
        device_id,
        rec_id,
        substation_id,
        feature_iri,
        'device/' || device_id                  as sensor_iri,
        '{{ celine_ns }}GridImportEnergy'       as observed_property,
        consumption_kwh                         as value,
        '{{ unit_kwh }}'                        as unit
    from keyed
    where consumption_kwh is not null

    union all

    {#
        Allocated, not metered — no sensor made it. Publishing a sensor here
        would claim the community's shared-energy split was measured at the
        member's meter, which is exactly what it is not.

        The allocation is pro-rata on the 15-minute pool. If the community
        figure ever moves to hourly netting, member shares stop summing to the
        community total unless this split moves with it.
    #}
    select
        md5(_id || '|VirtualConsumedEnergy'),
        ts,
        device_id,
        rec_id,
        substation_id,
        feature_iri,
        cast(null as text),
        '{{ celine_ns }}VirtualConsumedEnergy',
        virtual_consumption_kwh,
        '{{ unit_kwh }}'
    from keyed
    where virtual_consumption_kwh is not null

)

select * from unpivoted
