# dso_metering

The generic model for a **grid operator's own quarter-hour meter readings**, and for
releasing them to a party the person behind the supply point has named.

A distribution system operator holds these readings because it operates the meter. It
does not hold — and this model does not ask it for — any statement about who lives
behind a supply point. That is why the only identifier here is the supply point itself.

## What it produces

`ds_dev_gold.dso_meter_readings_15m` — one row per supply point and quarter-hour
interval:

| Column | Type | Description |
|--------|------|-------------|
| `_id` | text | `md5(pod_code \|\| ts)`, the merge key |
| `pod_code` | text | The supply point (POD). **The column a release is keyed and filtered on.** |
| `ts` | timestamptz | **Start** of the 15-minute interval, UTC |
| `consumption_kwh` | double precision | Energy drawn from the grid over the interval, kWh |
| `production_kwh` | double precision | Energy fed into the grid over the interval, kWh |
| `reading_quality` | text | `measured` / `estimated` / `substituted`, or null |

Energy per interval, never power: there is no kW→kWh conversion anywhere in this app or
downstream of it.

## Upstream dependency

Reads one silver table, `silver_dso_meter_readings`, produced by a deployment's own
private ingestion of the operator's export — not by this repository. The expected schema
is declared in `dbt/models/gold/sources.yml`, and the source schema is read from
`CELINE_SILVER_SCHEMA` (default `ds_dev_silver`).

**The export has to carry the supply point.** No join in this repository can supply it: a
meter serial or a gateway identifier is not a POD, and an operator's internal device id
is not something the party receiving the readings can be expected to know. A deployment
whose export carries something else has an upstream conversation to have, not a mapping
to write here — see "How the release is gated" below for what happens if it does not.

## How the release is gated

`governance.yaml` declares the dataset `pii`, `consent_required`, and filtered by:

```yaml
row_filters:
  - handler: subject_key_match
    args: {column: pod_code, key_type: pod}
```

The consent carries **typed data keys** — `pod:<value>` — registered by whoever collected
it. `subject_key_match` selects the keys of type `pod` and matches them against
`pod_code`. It resolves nothing and calls nobody at query time, which is the point: the
operator's data plane has no dependency on the community's registry.

**An empty intersection is a deny, not "no filter".** So a deployment whose table is
keyed by anything other than the values the consent carries serves **zero rows**, quietly
and correctly, forever. Zero rows returned is therefore never evidence that the filter
works — a test of this dataset has to be able to fail, which means at least one consent
whose key is present in the table and at least one whose key is not.

## What is not here, and why

- **No `meter_id`.** The metering device is the operator's internal affair; a release is
  about the supply point.
- **No member, household, contract or fiscal identifier.** Who holds a supply point is the
  community's fact and travels with the consent.
- **No `self_consumed_kwh`.** Behind-the-meter self-use is not metered at the connection
  point, so an operator cannot produce it.
- **No long, one-quantity-per-row projection yet.** SOSA and CIM's `IntervalBlock` both
  want one; the pattern for adding it is `rec_metering`'s `meters_measurements_15m` — a
  view sharing this dataset's governance block by YAML anchor, so the projection cannot
  lose the row filter. Adding it needs the `obs_energy_measurement` mapping spec to accept
  a supply-point-keyed observation, which it does not today.
- **No flow and no schedule.** There is nothing to orchestrate until a deployment's
  ingestion produces the silver table; the dbt model is the whole of this app.
