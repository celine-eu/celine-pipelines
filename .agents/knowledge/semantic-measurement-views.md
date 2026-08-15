# Why the `*_measurements_*` views exist, and the anchor that keeps them safe

Recorded 2026-08-09.

## A wide row cannot be one observation

`sosa:Observation` carries exactly one `sosa:observedProperty` — SSN states it as a cardinality
restriction — `saref:PropertyValue` one `saref:hasValue`, and `cim:IntervalReading` one value.
`meters_data_15m` carries three quantities and `rec_virtual_consumption_15m` four, so neither can
be published semantically as it stands.

This is not a gap in those standards. **One row with N quantities is N observations**, and the
fix is to unpivot rather than to look for a standard that tolerates the wide shape.

Three unpivoted **views** do that, all emitting the same ten columns — `observation_id,
result_time, device_id, rec_id, substation_id, feature_iri, sensor_iri, observed_property, value,
unit`:

| View | Source | Quantities |
|---|---|---|
| `ds_dev_gold.meters_measurements_15m` | `meters_data_15m` | import, export, self-consumed |
| `ds_dev_gold.rec_measurements_15m` | `rec_virtual_consumption_15m` | import, export, shared, ratio |
| `ds_dev_gold.rec_measurements_per_device_15m` | `rec_virtual_consumption_per_device_15m` | import, virtual consumed |

The identical column set is the point: it lets **one** mapping spec cover all three and any
future producer.

## The governance anchor is load-bearing

A semantic view is a projection of its source and must be exactly as readable. The governance
file makes that structural rather than conventional — a YAML anchor (`<<: *…`) makes the view's
entry and its source's entry **one object in the file**, so `access_level`, `ownership` and
`row_filters` cannot drift.

**This is not tidiness.** The dataset API applies whatever filters a dataset's *own* entry
declares and never compares two datasets. A view that lost its `rec_registry` filter would serve
every device's rows to every caller, and **nothing would fail**. Verified end to end: the filter
injects as `WHERE meters_measurements_15m.device_id IN (...)`, and source and view return 401
identically when unauthenticated.

For the same reason **`device_id` must survive into every view verbatim**. The filter names a
column and *nothing validates that the column exists on the table*, so a view that dropped or
renamed it would be served **unfiltered** rather than erroring.

`rec_measurements_15m` inherits the community models' missing row filter — see
`.agents/knowledge/rec-virtual-consumption.md`. The anchor means adding one to the source covers
the view in the same edit.

## Absolute IRIs in the data, deliberately

`observed_property`, `feature_iri`, `sensor_iri` and `unit` hold **full IRIs**, not bare keys or
CURIEs. A derived JSON-LD context can only say `"@type": "@id"` — it carries no `iri_template`,
and it declares only the prefixes its *targets* use — so a column holding `unit:KiloW-HR` or a
bare device code expands to a **relative** IRI against whatever URL the consumer fetched the
context from. Every other mapping spec has this defect; these views push the fix to the
producing side, which is the only place it can be fixed once.

## Status, and one dead end

**Not yet run.** The three views are syntax-checked (sqlglot, postgres dialect) but no `dbt run`
has executed them — the source tables are not in the local database. The end-to-end check used
hand-built fixture tables of the same shape.

The `owm` weather app already used this pattern and is where the shape was taken from, **but
`owm` is not in use** (confirmed 2026-08-09). Treat it as a worked example, not as something to
extend or keep in step. The live weather providers are `om` (Open-Meteo) and `mt`
(Meteotrentino), and nothing is gained by repairing `owm`'s unpivoted view.

## If the shape is extended to weather

`mt_stations` is an entity table, one station per row, and maps to `sosa:Platform` with no
reshaping — the cleanest target here after `grid_substations`. It needs a WGS84 registry entry
for `geo:lat` / `geo:long` / `geo:alt`. `mt_observations_current` is wide with seven quantities
and carries station code plus coordinates, so its feature of interest is real. The forecast
tables are **not** observations — `sosa:Observation` is the wrong class for a predicted value —
and `om_weather_hourly` has **no location column at all**, so it cannot be published honestly
until one is added.

The open question is which observable properties to use, and unlike REC energy the answer is
probably *not* CELINE: `quantitykind:Temperature` and friends already exist in QUDT, and WMO
publishes a dereferenceable code registry. `saref:Temperature` is deprecated as of SAREF core
v3.2.1 and is not the answer.

The energy mapping spec is named too narrowly for this. Its ten columns are already
domain-neutral — the `observed_property` IRI is the only thing making a row energy rather than
weather — so extending to weather is a rename plus a choice of vocabulary, not a second spec.
