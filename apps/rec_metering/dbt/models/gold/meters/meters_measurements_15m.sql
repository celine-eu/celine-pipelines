{{ config(materialized='view') }}

{#
    meters_data_15m, one quantity per row — the shape a semantic vocabulary can
    actually describe.

    `meters_data_15m` carries three quantities side by side. No observation
    vocabulary models that as one node: sosa:Observation carries exactly one
    sosa:observedProperty (SSN states it as a cardinality restriction),
    saref:PropertyValue exactly one saref:hasValue, cim:IntervalReading one
    value. One row with N quantities is N observations. This view is that
    unpivot, and `obs_energy_measurement.yaml` in celine-ontologies maps it.

    A view, not a table: it is a projection of meters_data_15m with no
    aggregation, so materialising it would duplicate the storage and add a
    second thing to keep fresh.

    ── Governance ──────────────────────────────────────────────────────────────
    `device_id` is carried verbatim and must stay that way. It is the column the
    `rec_registry` row filter matches on, and a view that dropped or renamed it
    would be served **unfiltered** — the filter is declared per dataset in
    governance.yaml, and nothing checks that the column it names exists. See the
    governance.yaml entry, which shares this table's block by YAML anchor for
    exactly that reason.

    ── Vocabulary IRIs absolute, entity IRIs relative ──────────────────────────
    `observed_property` and `unit` hold **full IRIs** rather than bare keys or
    CURIEs. A derived JSON-LD context can only say `"@type": "@id"` — it cannot
    carry a template, and it declares only the prefixes its targets use — so a
    column holding `unit:KiloW-HR` would expand against whatever URL the
    consumer fetched the context from. They name a CELINE term and a QUDT unit,
    which belong to whoever publishes those vocabularies and are not a
    deployment's to rename, so the full IRI is emitted here.

    `feature_iri` and `sensor_iri` are the opposite case and are emitted
    **relative** — `connection-point/CF1`, not `https://…/connection-point/CF1`.
    They identify the things *this deployment's data is about*, so the base
    belongs to whoever serves them: `dataset-api`'s `entity_base_uri`, and
    `MappingEngine._absolutize` prepends it to any value not already starting
    `http`. Hardcoding a base here — this view carried
    `https://w3id.org/celine-eu/id` until 2026-09-04 — produced graphs whose
    subjects sat under the deployment's base and whose object references sat
    under w3id, and no deployment could name its own entities (issue #5).

    Property IRIs are CELINE's (ontology v0.10) because no standard names them:
    quantitykind:Energy does not distinguish grid import from grid export, and
    saref:Energy is deprecated as of SAREF core v3.2.1. Units are QUDT's.
#}

{% set celine_ns = 'https://w3id.org/celine-eu#' %}
{% set unit_kwh  = 'http://qudt.org/vocab/unit/KiloW-HR' %}

with base as (
    select
        _id,
        device_id,
        ts,
        consumption_kwh,
        production_kwh,
        self_consumed_kwh
    from {{ ref('meters_data_15m') }}
),

{#
    The feature of interest is the connection point the meter serves, not the
    meter. This layer has no registry join, so the connection point is keyed by
    the device that serves it — stated here rather than left for a reader to
    infer from the IRI shape. The meter itself is the sensor.
#}
unpivoted as (

    select
        md5(_id || '|GridImportEnergy')                   as observation_id,
        ts                                                as result_time,
        device_id,
        cast(null as text)                                as rec_id,
        cast(null as text)                                as substation_id,
        'connection-point/' || device_id                  as feature_iri,
        'device/' || device_id                            as sensor_iri,
        '{{ celine_ns }}GridImportEnergy'                 as observed_property,
        consumption_kwh                                   as value,
        '{{ unit_kwh }}'                                  as unit
    from base
    where consumption_kwh is not null

    union all

    select
        md5(_id || '|GridExportEnergy'),
        ts,
        device_id,
        cast(null as text),
        cast(null as text),
        'connection-point/' || device_id,
        'device/' || device_id,
        '{{ celine_ns }}GridExportEnergy',
        production_kwh,
        '{{ unit_kwh }}'
    from base
    where production_kwh is not null

    union all

    {#
        Behind-the-meter self-use, distinct from the community's shared energy.
        Known to be negative on ~12% of rows upstream: the private ingestion
        pipeline that produces meters_data_normalized computes it without a
        max(...,0) clamp. That is a data defect this view neither introduces nor
        hides — it publishes the column as it stands.
    #}
    select
        md5(_id || '|SelfConsumedEnergy'),
        ts,
        device_id,
        cast(null as text),
        cast(null as text),
        'connection-point/' || device_id,
        'device/' || device_id,
        '{{ celine_ns }}SelfConsumedEnergy',
        self_consumed_kwh,
        '{{ unit_kwh }}'
    from base
    where self_consumed_kwh is not null

)

select * from unpivoted
