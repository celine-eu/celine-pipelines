# References — what this repository refers to but must not commit

This repository is open source. Some of what an agent needs in order to work here is a
fact about one machine, or about a deployment that is not public. Neither belongs in a
committed file.

This file is the **register**: it declares each name and what kind it is, and never the
value (REQ-0010). The values live in `references.local.md`, which is gitignored
(REQ-0011). Documents cite a name in `{{DOUBLE_BRACES}}`.

| Kind | Meaning |
|---|---|
| **local** | true of one machine, meaningless on another — a checkout location, a port, a hostname |
| **restricted** | true everywhere and publishable nowhere — a deployment, an organisation, a customer, a person |

---

## Declared names

The list below is what the conformance checker reads (`- NAME: kind — meaning`); the
prose under each heading is for people. **Keep both in step** — a name that appears only
as a heading is undeclared as far as REQ-0010 is concerned, and the local value beside it
is then reported as undeclared.

- `UPSTREAM_PIPELINES_ROOT`: restricted — the checkout path of the private deployment
  repository that produces this repository's upstream `raw` and `silver` tables. It is
  *also* machine-bound, so `local` applies too; **the stronger kind is the one declared**,
  because committing it would be a disclosure rather than merely noise.

### `{{UPSTREAM_PIPELINES_ROOT}}` — **local** and **restricted**

The checkout path of the private deployment repository whose pipelines produce the
upstream `raw` and `silver` tables that `rec_metering`, `grid` and `rec_flexibility`
consume. See
[`knowledge/upstream-tables-have-external-producers.md`](knowledge/upstream-tables-have-external-producers.md)
for what lives there and how to triage a failure against it.

Both kinds apply, and that is the point: the path is machine-bound *and* it names a
deployment. Either alone would be enough to keep it out of a commit.

---

## Setting it up

Copy the template and fill in the value for your machine:

```bash
cp .agents/references.local.md.example .agents/references.local.md
$EDITOR .agents/references.local.md
```

`references.local.md` is gitignored. If `git status` ever shows it, stop and fix the
ignore rule before committing anything.

## Rules that hold regardless

- **An absolute home directory in a committed file is a violation with or without this
  register** (REQ-0303). Declaring a name does not license writing the value somewhere
  else.
- **A declared value appearing in committed material is a violation** (REQ-0304). That
  includes code comments, dbt model headers, test fixtures and commit messages.
- **Most local facts want deleting, not declaring.** Only add a name here when a document
  genuinely cannot say what it means without one. Where an interpreter lives is
  discoverable in a second; a name for it buys indirection and no information.
- Describe a private upstream **by its role** — "the pipeline that produces
  `meters_data_normalized`" — not by its name. Table names consumed here are already
  public: they are declared in this repository's own `sources.yml` files.
