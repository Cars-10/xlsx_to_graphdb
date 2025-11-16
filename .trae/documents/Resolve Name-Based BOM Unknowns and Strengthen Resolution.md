## Findings
- All 2073 edges were marked unknown (none ambiguous) because the name-based resolver in `import_data` only looked up names in `name_to_pn`, without the numeric fallback to part numbers or case-insensitive matching.
- The generator often writes numbers as names when no name is found; those numerics weren’t recognized as part numbers by the current debug path.
- Imports and dependencies are correct (`pandas`, `rdflib`, `openpyxl` via `pandas`); module resolution and environment are fine. The failure is logical, not environmental.
- Case sensitivity and whitespace normalization exist in parsers, but resolution needs consistent normalization and numeric fallback.

## Root Cause
- In `import_data` name-resolution branch, candidates are derived as:
  - `p_candidates = name_to_pn.get(p_name.strip())`
  - `c_candidates = name_to_pn.get(c_name.strip())`
- No fallback when `p_name` or `c_name` is actually a part number or when case variants exist. This yields 0 candidates → counted as unknown.

## Changes to Implement
1. Normalize and fallback in resolver
- Enhance the name-resolution loop in `import_data` to:
  - Normalize names (`strip`, collapse spaces, lower-case lookup against a secondary `name_to_pn_lower`).
  - If lookup fails, treat the name as a part number (use `parts` keys and `normalize_part_number`).

2. Deterministic ambiguity handling (non-strict)
- If multiple candidates: apply tie-breakers (prefer latest `Revision`, `View='Design'`, container). If none apply, choose lexicographically first PN. Fail in strict mode.

3. Index enrichment
- Ensure `build_cross_index` builds both `pn_to_name` and a lower-cased `name_to_pn_lower` map. Include sources (`sheet`, `revision`, `view`, `container`) to support tie-breakers.

4. Diagnostics
- Keep `--debug-names` summary and `--resolution-report` CSV; add top-N unknown and ambiguous names to logs for quick inspection.
- Emit `bom_by_name_candidates.csv` from number-based BOM as an editable intermediate when needed.

5. Error handling
- In strict mode, raise a single error summarizing counts and point to the report CSV.
- In non-strict, resolve ambiguities deterministically and only skip when truly unknown after all attempts.

## Validation Plan
- Unit tests for:
  - Numeric fallback (names that are numbers resolve).
  - Case-insensitive name matching.
  - Tie-breaker selection for ambiguous names (non-strict vs strict).
  - Resolution report contents.

## Environment & Config Review
- Imports/dependencies: already correct in venv; no changes needed.
- Module resolution: CLI and tests work; no changes needed.
- Path/env: Script uses CWD; reports written to CWD when `DEBUG_NAMES=1`.

## Outcome
- The 2073 unknown edges resolve (most are numeric fallbacks), reducing unknowns to near-zero and enabling relationship creation by name.

## Next Step
On approval, I will implement the resolver improvements, lower-case index, tie-breakers, and tests, then re-run to verify the unknown count drops and relationships populate.