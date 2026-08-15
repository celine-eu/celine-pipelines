# Playbook — publishing a documentation page

Adding a page under `docs/` is **two commits in two repositories**. Doing only the first
one produces a page that is fetched, built and never linked — and the site build stays
green while it happens, so nothing tells you.

| Repository | What it owns |
|---|---|
| this one | the content: `README.md`, `apps/*/README.md`, `docs/**` |
| `celine-eu.github.io` | `repos.yaml` — which files are fetched, and the nav that links them |

## What the site does with this repository

`scripts/build.py` in `celine-eu.github.io` clones this repository **from GitHub `main`**,
copies the files matching the `paths:` block, and generates the MkDocs nav from the `nav:`
block. Two conversions matter:

- **`README.md` always becomes `index.md`** — in every directory, not just the root. So
  `apps/grid/README.md` is referenced in the nav as `apps/grid/index.md`.
- Nav entries are relative to this repository's root and are prefixed with
  `projects/celine-pipelines/` by the builder. The bare string `README` is special-cased
  to the project's landing page and titled "Overview".

## The procedure

### 1. Write the page

Put it in `docs/`. Link it from the sibling pages that should lead to it — the nav is not
the only way in, and a page reachable only from the sidebar reads as an orphan.

### 2. Update `paths:` if the file is somewhere new

Only needed for a genuinely new location. `docs/**`, `README.md` and `apps/*/README.md`
are already covered.

If you do change `paths:`, change `.github/workflows/update-docs.yaml` in **this**
repository to match. That workflow is what tells the site to rebuild, and its `paths:`
filter and `repos.yaml`'s `paths:` block are two halves of one statement — a file the site
fetches but the workflow ignores is published only when something else happens to trigger
a build.

### 3. Add the nav entry in `celine-eu.github.io/repos.yaml`

Under the `celine-pipelines` entry:

```yaml
    nav:
      - README
      - Pipeline Overview: docs/pipeline-overview.md
      - Testing: docs/testing.md            # <- explicit title, explicit path
      - Grid Resilience: apps/grid/index.md  # <- an app README
```

Prefer the explicit `Title: path` form. The bare-path form derives a title by title-casing
the filename, which gives "Local Runtime" for `local-runtime.md` but something worse for
most names.

### 4. Order the two commits

The site clones `main`. A nav entry merged before the page it points at is a broken entry
for however long that gap lasts. Either merge this repository first, or merge both
together.

### 5. Verify

`celine-eu.github.io` builds locally without credentials:

```bash
cd ../celine-eu.github.io
task rebuild          # clean, re-clone every repo, build
task serve            # http://127.0.0.1:9901
```

`task rebuild` fetches from GitHub, so **it will not see uncommitted local work**. To
check a page before pushing, copy it into the working clone by hand:

```bash
cp docs/new-page.md ../celine-eu.github.io/.work/celine-pipelines/docs/
cd ../celine-eu.github.io && uv run python scripts/build.py
```

## The failure mode to watch for

`mkdocs.tpl.yml` does not set `strict: true`. A nav entry pointing at a file that was not
fetched therefore produces:

```text
WARNING - A reference to 'projects/celine-pipelines/docs/x.md' is included in the
          'nav' configuration, which is not found in the documentation files.
```

…and the build **succeeds**. The page is simply absent from the sidebar. So:

- Read the build output. A green exit code is not evidence the page is published.
- Check the rendered sidebar, not just that the file exists in `site/`.

The same silence covers the opposite mistake — a file fetched but never named in `nav:` is
copied into the site and linked from nowhere.

## Checks worth running

```bash
cd ../celine-eu.github.io
task docs:stale              # flags repos whose code moved but whose docs did not
```

## What must never be published

Everything under `docs/` and every `README.md` here is public. Nothing naming a
deployment, an organisation, a customer, a person, or a private repository belongs in
them — including in a code comment or a spec link that only resolves inside a private
checkout. Describe a private upstream by its role ("the private ingestion pipeline that
produces `meters_data_normalized`"), never by its name.
