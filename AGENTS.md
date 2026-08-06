# AGENTS.md

Guidance for agentic coding tools (Claude, Codemie Code, Copilot, Cursor, etc.)
working in this repository.

## Repository Overview

This repo hosts the **OHDSI Health Systems Interest Group (HSIG)** website and
supporting SQL artifacts. It is **not a software application/library** — there
is no package manager, no application code, and no automated test suite.

- `docs/` — [Quarto](https://quarto.org) website source. Rendered output is
  published to GitHub Pages via `.github/workflows/publish.yml`.
  - `docs/*.qmd`, `docs/**/*.qmd` — page content (Quarto Markdown).
  - `docs/_quarto.yml` — site configuration (nav, sidebar, theme, footer).
  - `docs/presentations/presentations.yml` — data file that drives the
    Presentations page listing.
  - `docs/_extensions/` — third-party Quarto Lua/JS extensions (iconify, d2,
    webr, qrcode). **Do not hand-edit these** — they are vendored extensions.
  - `docs/src/` — Quarto's rendered HTML output (**generated**, do not edit
    directly; also largely covered by `.gitignore`).
- `inst/sql/snowflake/` — standalone Snowflake SQL scripts used by community
  members for OMOP CDM ETL tasks (e.g. `SOURCE_TO_CONCEPT_MAP` →
  `CONCEPT`/`CONCEPT_RELATIONSHIP` sync). These are reference scripts, not
  part of an R package or build pipeline.
- No `.cursor/rules/`, `.cursorrules`, or `.github/copilot-instructions.md`
  files exist in this repo. If any are added later, their rules should be
  merged into this file.

## Build / Render Commands

There is no `npm`/`make`/`R CMD` build. The only build step is rendering the
Quarto site, and it requires the Quarto CLI to be installed locally
(https://quarto.org/docs/get-started/).

```bash
# Render the full site (output goes to docs/src, per docs/_quarto.yml)
quarto render docs

# Live preview a single page while editing (auto-reload)
quarto preview docs/index.qmd

# Preview the whole site locally on port 8000 (see docs/_quarto.yml)
quarto preview docs

# Render just one page (useful when iterating on a single .qmd file)
quarto render docs/resources.qmd
```

CI (`.github/workflows/publish.yml`) runs `quarto render` against `docs/src`
on every push to `main` and publishes the result to the `gh-pages` branch.
There is no separate lint or test job in CI — rendering success is the only
validation gate.

**Manual CI Trigger (bypass push triggers):**
If GitHub is experiencing webhook delays or Actions outages, admins can manually trigger the publishing pipeline:
```bash
# Using the GitHub CLI
gh workflow run publish.yml --ref main
```

## Testing

**There are no unit/integration tests in this repository.** "Testing" a
change means:

1. Render the specific page(s) you touched and confirm no Quarto/Pandoc
   errors or warnings (e.g. unclosed divs, broken YAML front matter):
   ```bash
   quarto render docs/path/to/page.qmd
   ```
2. For SQL scripts under `inst/sql/`, there is no CI execution — validate by
   manually reviewing the SQL against a Snowflake OMOP CDM instance if you
   have access. There is no mocked/test database in this repo.
3. If you change `docs/_quarto.yml` or `docs/presentations/presentations.yml`,
   render the *entire* site (`quarto render docs`) since these are global
   config/data files that affect navigation/sidebars across pages.

If a future contributor adds a real test suite (e.g. R testthat, SQL unit
tests), add the exact single-test invocation command here.

## Linting / Formatting

No linter or formatter is configured (no `.editorconfig`, no markdownlint
config, no SQL formatter config). Follow the conventions already present in
neighboring files rather than introducing a new style.

## Code Style Guidelines

### Quarto/Markdown (`.qmd` files)
- Start every page with YAML front matter (`---`), even if minimal:
  ```yaml
  ---
  title: "Page Title"
  ---
  ```
  Pages without a right-hand TOC use `hide: [toc]` (see
  `docs/resources/proprietary.qmd`, `docs/contribute/submit_mappings.qmd`).
- Use `##`/`###` (not `#`) for in-page section headers; reserve `#` for the
  document title when no YAML `title` is set.
- Bullet lists in prose sections use the `•` character followed by a tab
  (matches existing convention in `getting-started.qmd`, `Overview.qmd`) —
  match this style when adding to those specific pages; standard Markdown
  `-`/`*` bullets are fine elsewhere.
- Internal links are relative and use the `.qmd` extension, e.g.
  `[Proprietary and PHI considerations](../resources/proprietary.qmd)`.
- Mark unfinished/placeholder content with an HTML comment `<!-- TODO: ... -->`
  referencing a GitHub issue number when one exists, e.g.
  `<!-- TODO: add guidance ... (see Issue #7) -->`.
- Do not commit rendered output — `docs/src/**/*.html`, `docs/.quarto/`, and
  `docs/search.json` are generated and gitignored; never hand-edit generated
  HTML.

### YAML (`_quarto.yml`, `presentations.yml`)
- 2-space indentation, no tabs.
- Quote string values that contain colons, apostrophes, or start with special
  characters (e.g. titles with `:`) to avoid YAML parse errors — see the fix
  history in this repo (`9ced68a Fix YAML presentation titles by adding quotes`).
- Keep list entries (`tiles:`, `contents:`) alphabetized/chronological as
  found in the surrounding block; don't reorder unrelated entries in the same
  edit.

### SQL (`inst/sql/`)
- Target dialect is **Snowflake** unless a script explicitly states otherwise.
- Use ALL CAPS for SQL keywords and table/column names (matches existing
  scripts), e.g. `SELECT`, `INNER JOIN`, `CONCEPT_ID`.
- Prefix custom/local vocabulary identifiers consistently (existing scripts
  use a configurable `CUSTOM_SOURCE_VOCABULARY_ID_PREFIX` SQL variable) —
  don't hardcode organization-specific prefixes like `CH_` in new shared
  scripts; make them a `SET` variable at the top instead.
- Include a header comment block with: script name, author(s), organization,
  last-revised date, and a plain-language description of what the script
  does and any known limitations (see
  `inst/sql/snowflake/STCM_to_CCR_Snowflake_v1.sql` for the pattern).
- Prefer `MERGE`/temp-table snapshot patterns already used in existing
  scripts (drop-then-insert via a temporary table) over ad hoc `UPDATE`
  statements, to stay consistent with the existing idempotent-refresh style.

### General
- No application code, so there are no import/type/naming conventions for a
  programming language to enforce here. If code (R, Python, etc.) is
  eventually added to this repo, prefer the tidyverse style guide for R or
  PEP 8 for Python, and update this file with concrete commands.
- Keep prose factual and community-neutral (this is an OHDSI working-group
  site); avoid promotional language when editing page content.
- Commit messages in this repo are short, imperative, present-tense
  (`Fix date formatting in presentations.yml`, `Remove engine: knitr from
  pages with no R code`) — follow this style.
