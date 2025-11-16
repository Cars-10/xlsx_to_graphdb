## Goal
Eliminate unknown/ambiguous names by building a comprehensive Number→Name index from all relevant tabs before generating/importing the name-based BOM.

## Columns To Use
- Parts tabs: `Number`, `Name` (primary source)
- BOMSheet1: `Number` (parent), `Component Id` (child); enrich by mapping these numbers to names
- Alternate Link tab: `Parent Part Number`, `Child Part Number`, `Replacement Part Number`; enrich by mapping these numbers to names
- Optional context: `Revision`, `View`, `Container`, `Organization ID` for tie-breakers when duplicate names exist

## Implementation
1. Cross-sheet index builder
- Scan all sheets; when both `Number` and `Name` exist, add to `pn_to_name`
- Also collect `Replacement Part Number`, `Parent Part Number`, `Child Part Number`, and `Component Id` numbers into the index, resolving names via any parts sheets found
- Normalize names (trim, case) and record duplicates: `name_to_pn[name] → [pns]`

2. Tie-breakers for duplicates (configurable)
- Prefer entries with latest `Revision`
- Prefer `View='Design'` (if present) or a configured `preferred_view`
- Prefer rows from a configured `preferred_container`
- If still ambiguous, require strict mode to fail or choose the first deterministically and log

3. Name BOM generation
- When creating `bom_by_name.csv`, map numbers using the enriched index; avoid fallbacks where possible
- Include a report CSV (e.g., `bom_name_resolution_report.csv`) listing unresolved or ambiguous items and applied tie-breakers

4. Import resolution
- When importing by names, use the enriched `name_to_pn` and tie-breaker policy to resolve names deterministically
- Keep `--strict-names` to fail if ambiguity remains

5. CLI additions
- `--prefer-view`, `--prefer-container`, `--prefer-latest-revision` flags
- `--resolution-report path` to write the name resolution diagnostics

6. Tests
- Build fixture sheets covering: parts with `Number/Name`, BOMSheet1 with `Number/Component Id`, alternate link sheet
- Verify index merges correctly, BOM-by-name generation yields names, and import creates relationships
- Verify tie-breaker selection and strict failure on unresolved cases

7. Docs
- Document columns used, tie-breakers, flags, and the resolution report format in `IMPORTING.md`

## Outcome
- Name-based BOM edges will resolve cleanly using numbers from BOM tabs mapped to names via parts tabs, minimizing fallbacks and warnings.

## Proceed
On approval, I will implement the index builder, tie-breakers, generator/import changes, tests, and documentation, then run verification.