{{ config(materialized='table', schema='gold') }}

{#
    Tree-strike exposure spans as their own map layer (static overlay).

    One row per span of the wind_tree_strike LiDAR analysis (~3.6k overhead
    spans, median ~100 m), with its own geometry — finer than the 1,178 map
    tratte, whose strike_tree_tier is the worst intersecting span.

    span_id is minted here from the span's line, municipality, conductor type
    and geometry: the research export's positional seg_id renumbers on every
    regeneration and is absent from older ingestions, so it is not relied on.
    Exact duplicates (same attributes and geometry) collapse to one row.

    dso_id is the operator the silver row is stamped with: provenance is the
    export file, never geometry. operational_unit, feeder and primary
    substation are taken from the nearest CIM segment of the same line within
    50 m (spans and segments come from the same LineeMT source through
    different CRS paths); a span with no such neighbour keeps the export's
    feeder/substation and a NULL unit. A nearest segment of another operator
    fails the test grid_tree_strike_spans_dso_matches_nearest_segment.

    Exposure only: wind risk escalation still runs on the tratte
    (grid_wind_risks) and is unchanged by this model.
#}

{% set spans = source('grid_silver', 'silver_grid_geo_tree_strike') %}
{% set seg   = source('grid_silver', 'silver_grid_ac_line_segment') %}

with deduped as (

    select distinct on (dso_id, nomelinea, comune, tipologia, md5(ST_AsEWKB(geom)::text))
        dso_id,
        nomelinea,
        feederid,
        sottostazi,
        comune,
        tipologia,
        n_strike,
        strike_density_km,
        tier,
        multiplier,
        geom
    from {{ spans }}
    where geom is not null
    order by dso_id, nomelinea, comune, tipologia, md5(ST_AsEWKB(geom)::text), multiplier desc

),

mapped as (

    select
        d.*,
        case d.tipologia
            when 'Aereo Nudo'    then 'overhead_bare'
            when 'Aerea'         then 'overhead_bare'
            when 'Cavo Aereo'    then 'overhead_insulated'
            when 'Cavo Interrato' then 'underground_cable'
            else lower(replace(d.tipologia, ' ', '_'))
        end as conductor_type
    from deduped d

),

annotated as (

    select
        m.*,
        near.operational_unit       as seg_operational_unit,
        near.feeder_id              as seg_feeder_id,
        near.parent_substation_name as seg_parent_substation_name
    from mapped m
    left join lateral (
        select s.operational_unit, s.feeder_id, s.parent_substation_name
        from {{ seg }} s
        where s.line_name = m.nomelinea
          and ST_DWithin(s.geom, m.geom, 50)
        order by ST_Distance(s.geom, m.geom) asc
        limit 1
    ) near on true

),

final as (

    select
        md5(
            dso_id || '|' || 'tree_strike_span' || '|' ||
            nomelinea || '|' || comune || '|' || conductor_type || '|' ||
            md5(ST_AsEWKB(geom)::text)
        )                                                   as span_id,
        dso_id,
        nomelinea                                           as line_name,
        coalesce(seg_feeder_id, feederid)                   as feeder_id,
        coalesce(seg_parent_substation_name, sottostazi)    as parent_substation_name,
        seg_operational_unit                                as operational_unit,
        comune                                              as municipality,
        conductor_type,
        n_strike,
        strike_density_km,
        tier,
        multiplier,
        ST_Length(geom)                                     as length_m,
        geom
    from annotated

)

select
    span_id,
    dso_id,
    line_name,
    feeder_id,
    parent_substation_name,
    operational_unit,
    municipality,
    conductor_type,
    n_strike,
    strike_density_km,
    tier,
    multiplier,
    length_m,
    geom,
    {{ grid_geojson_feature(
        geom_col         = 'geom',
        geometry_type    = 'line',
        extra_props_expr = "json_build_object(
            'span_id',                span_id,
            'line_name',              line_name,
            'municipality',           municipality,
            'operational_unit',       operational_unit,
            'parent_substation_name', parent_substation_name,
            'conductor_type',         conductor_type,
            'tier',                   tier,
            'multiplier',             multiplier,
            'strike_density_km',      strike_density_km,
            'n_strike',               n_strike,
            'length_m',               length_m
        )"
    ) }} as feature_geojson
from final
