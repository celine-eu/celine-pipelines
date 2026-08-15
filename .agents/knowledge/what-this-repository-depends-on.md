# What this repository depends on, and what depends on it

The minimum perimeter for working here. Paths are written `../<repo>`, which resolves when
this repository is checked out inside the `celine-dev` workspace and not otherwise.

**This repository is upstream of nearly everything**, which makes the *outbound* direction
the one that matters. `docs/pipeline-overview.md` describes the layers and the governance
format; this entry says who is on the other side of them.

**Working on this repository alone, your visibility is limited to it.** Nothing here fails
when a consumer breaks. If a change moves a gold table or a `governance.yaml` entry, get
the `celine-dev` workspace and read the component-model entry in its `.agents/knowledge/` —
named rather than linked, because a path into the workspace does not resolve from inside a
member.

## Consumed

| What | Owned by | How it arrives |
|---|---|---|
| lineage tracking, `governance.yaml` loading, dbt/Meltano wrappers | `../celine-utils` | the base image and a package dependency |
| the `governance.yaml` schema | `../celine-utils` | published; validated against, not vendored |
| the `ownership` value mapping | `../dataset-api` | its `owners.yaml`, read at governance sync |
| upstream `raw` and `silver` tables for `rec_metering`, `grid`, `rec_flexibility` | **a private deployment repository** | at `{{UPSTREAM_PIPELINES_ROOT}}` — see `.agents/references.md` |

The last row is the one that surprises people: some pipelines here consume tables this
repository does not produce, from a repository that is not public. A missing table or
column is therefore often **not this repository's fault**. Triage before assuming —
`upstream-tables-have-external-producers.md`, beside this file.

## Consumed by

Gold tables are stable interfaces, and that is the whole point of the layer:

| Consumer | Takes |
|---|---|
| `../dataset-api` | serves whatever `governance.yaml` declares as exposed |
| `../digital-twin` | gold tables, through `dataset-api` |
| `../celine-dashboards` | gold tables, and the `governance.yaml` files themselves |
| `../celine-forecasting` | silver meter and weather tables, read directly |
| `../celine-eu.github.io` | this repository's `docs/`, published to the public site |

**A change to a gold table is a change to every one of those**, and none of them will fail
in this repository's CI. That is what makes gold an interface rather than an output.

## Which seams this repository sits on

Three of the five, and it is the **owner** of two:

- **Data schema** — it defines the layers. Renaming a gold column is an interface change.
- **Governance metadata** — `governance.yaml` is authored here and consumed by
  `dataset-api` and `celine-dashboards`. Each pipeline declares only the datasets it
  produces; a dataset declared twice has two answers to "who owns this"
  (`docs/pipeline-overview.md`).
- **API contract** — inbound only, and indirect: it does not call services, but what it
  publishes becomes `dataset-api`'s surface.

It makes no identity or policy decision and maps nothing to an ontology.

## The publication boundary

`docs/` here is copied verbatim onto the **public** documentation site by
`../celine-eu.github.io`. A private name, path or dataset written into a document in this
repository becomes public at the next site build, with no further review. Write `docs/` as
though it is already published, because it is.
