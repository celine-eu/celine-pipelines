{{ config(materialized='view') }}

{#
    rec_virtual_consumption_15m, one quantity per row.

    Four quantities per source row, so four observations. See
    `meters_measurements_15m` in apps/rec_metering for why a wide row cannot be
    one observation node in any of SOSA, SAREF or CIM, and
    `obs_energy_measurement.yaml` in celine-ontologies for the mapping.

    ── The feature of interest is the sharing group, not the community ─────────
    The source model's grain is `(ts, rec_id, substation_id)` because netting is
    per primary substation: sharing cannot cross an unconnected cabina primaria,
    and a community-wide net would invent energy that physically cannot flow.
    Collapsing the feature to the REC would throw that away and mint one IRI for
    several distinct rows. celine:SharingGroup is the class CELINE defines for a
    community partition accounted within one regulatory zone, so that is what
    the IRI names.

    ── Column names versus meaning ─────────────────────────────────────────────
    The source column names are misleading and this view is where that stops
    mattering to a consumer:

      total_consumption_kwh  is community *grid import*   (prelievo)
      total_production_kwh   is community *grid export*   (immissione)
      self_consumption_kwh   is *collectively shared*     (not anyone's self-use)
      self_consumption_ratio is shared over *export*      (not over import)

    The last one is not summable across substations, and celine-webapp publishes
    a different figure under the same name with import as the denominator. The
    observable property's rdfs:comment in ontology v0.10 states the denominator
    so a consumer does not have to guess.

    ── Governance ──────────────────────────────────────────────────────────────
    The source model carries **no row filter** — a known gap, latent until a
    second REC exists, recorded in celine-dev/.agents/celine-pipelines/FACTS.md.
    This view inherits that gap and does not widen it: `rec_id` is carried
    verbatim so a filter can be added to both at once. Its governance entry
    shares the source's block by YAML anchor.
#}

{% set celine_id = 'https://w3id.org/celine-eu/id' %}
{% set celine_ns = 'https://w3id.org/celine-eu#' %}
{% set unit_kwh  = 'http://qudt.org/vocab/unit/KiloW-HR' %}
{% set unitless  = 'http://qudt.org/vocab/unit/UNITLESS' %}

with base as (
    select
        ts,
        rec_id,
        substation_id,
        total_consumption_kwh,
        total_production_kwh,
        self_consumption_kwh,
        self_consumption_ratio
    from {{ ref('rec_virtual_consumption_15m') }}
),

{#
    The grain hashed into observation_id is the model's full grain plus the
    property. Dropping substation_id would mint one id for several rows, which
    is the defect obs_rec_energy's id_template carried until 2026-08-09.
#}
keyed as (
    select
        *,
        rec_id || '|' || substation_id || '|' || ts::text        as grain,
        '{{ celine_id }}/sharing-group/' || rec_id || '/' || substation_id
                                                                 as feature_iri
    from base
),

unpivoted as (

    select
        md5(grain || '|GridImportEnergy')   as observation_id,
        ts                                  as result_time,
        cast(null as text)                  as device_id,
        rec_id,
        substation_id,
        feature_iri,
        {# No sensor: an aggregate over a substation is made by no single one,
           and asserting one would invent a provenance the data lacks. #}
        cast(null as text)                  as sensor_iri,
        '{{ celine_ns }}GridImportEnergy'   as observed_property,
        total_consumption_kwh               as value,
        '{{ unit_kwh }}'                    as unit
    from keyed
    where total_consumption_kwh is not null

    union all

    select
        md5(grain || '|GridExportEnergy'),
        ts, cast(null as text), rec_id, substation_id, feature_iri,
        cast(null as text),
        '{{ celine_ns }}GridExportEnergy',
        total_production_kwh,
        '{{ unit_kwh }}'
    from keyed
    where total_production_kwh is not null

    union all

    select
        md5(grain || '|CollectivelySharedEnergy'),
        ts, cast(null as text), rec_id, substation_id, feature_iri,
        cast(null as text),
        -- Not celine:SharedEnergy. That IRI belongs to the period-total KPI
        -- concept in celine:KPICatalog, released in ontology v0.5; the
        -- observable property this column reports is
        -- celine:CollectivelySharedEnergy, added in v0.10.
        '{{ celine_ns }}CollectivelySharedEnergy',
        self_consumption_kwh,
        '{{ unit_kwh }}'
    from keyed
    where self_consumption_kwh is not null

    union all

    select
        md5(grain || '|SelfConsumptionRatio'),
        ts, cast(null as text), rec_id, substation_id, feature_iri,
        cast(null as text),
        '{{ celine_ns }}SelfConsumptionRatio',
        self_consumption_ratio,
        '{{ unitless }}'
    from keyed
    where self_consumption_ratio is not null

)

select * from unpivoted
