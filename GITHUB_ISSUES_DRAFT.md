# Draft GitHub Issues — migrated from docs/TODO.md backlog

File these under https://github.com/OHDSI/HealthSystems/issues
(delete this file once filed — it is a temporary staging note, not part of the site)

---

## Issue: Add support for storing Word document uploads

**Body:**
The site currently has no defined way for community members to share Word
document artifacts (e.g. mapping documentation, meeting notes). Decide on a
storage location/mechanism (e.g. SharePoint link, GitHub repo folder) and
document it on the [Contribute](../docs/contribute/submit_mappings.qmd) page.

Originally tracked in `docs/TODO.md`.

---

## Issue: Add support for storing CSV file uploads

**Body:**
Same as the Word document issue above, but for CSV files (e.g. source-to-
concept mapping exports). Decide on a storage location/mechanism and document
it on the Contribute page.

Originally tracked in `docs/TODO.md`.

---

## Issue: Link to Dynamic Mapping Tools and their metadata

**Body:**
Add a link to "Dynamic Mapping Tools" and associated metadata resources once
identified. Unclear from the original backlog note which specific tool(s)
this refers to — needs scoping with the HSIG group before content can be
written. Likely destination: `docs/resources.qmd` or a new page under
`docs/etl-mapping/`.

Originally tracked in `docs/TODO.md`.

---

## Issue: Add EHR source-system guidance beyond Epic

**Body:**
`docs/etl-mapping/ehr-source-systems.qmd` currently only has real content for
Epic. Add guidance/links for:
- Cerner / Oracle Health
- athenahealth
- Other proprietary or open-source EHRs (OpenMRS, OpenEMR, etc.)

Placeholder section headers already exist in the page; this issue tracks
writing the actual content.

Originally tracked in `docs/TODO.md` and inline TODO comments in
`getting-started.qmd` (pre-rebuild).

---

## Issue: Write terminology-mapping guidance for common vocabularies

**Body:**
`docs/etl-mapping/terminology-mapping/terminology-mapping.qmd` links to
USAGI/Athena/Themis but does not yet contain specific guidance on mapping
common interface terminologies (SNOMED CT, LOINC, RxNorm, ICD, etc.) to OMOP
standard vocabularies. Write this guidance.

Originally tracked as an inline TODO comment in `getting-started.qmd`
(pre-rebuild).

---

## Issue: Add Inventory Management guidance/content

**Body:**
Original backlog item was just "Inventory Management" with no further detail.
Needs scoping with the HSIG group to determine what this refers to (e.g.
supply-chain/inventory data ETL to OMOP?) before content can be written.

Originally tracked in `docs/TODO.md`.

---

## Issue: Populate OpenMRS and OpenEMR OMOP mapping links

**Body:**
`docs/resources.qmd` currently notes that OpenMRS and OpenEMR OMOP mapping
guidance is "not yet available." Previously these were dead `#` placeholder
links in `docs/resources/Overview.qmd`. Populate with real links once
community-contributed guidance exists.

Originally tracked as inline TODO comments in `docs/resources/Overview.qmd`
(pre-rebuild).
