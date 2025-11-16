## Goal
Make the name-based BOM CSV reliably populated and consumed so relationships are created when importing by names.

## Changes
1. Robust generation
- Update generator to always emit rows for each number-based edge, mapping numbers→names and falling back to numbers if names are missing.
- Trim whitespace and normalize case when mapping names.
- Log generated row count and fallback usage.

2. Script regeneration logic
- Enhance `load_by_name.sh` to:
  - Regenerate `bom_by_name.csv` if it is missing OR empty (0 bytes) OR has 0 data rows.
  - Add `FORCE_GENERATE_BOM_BY_NAME=1` env to overwrite any existing `bom_by_name.csv`.

3. Name BOM parser
- Accept `Parent Name`/`Child Name` in any order and case-insensitive.
- Strip leading/trailing spaces; skip entirely blank rows.

4. CLI options
- Add `--force-generate-bom-by-name` to overwrite an existing name-based BOM.
- Keep `--out-bom-name` for output path.

5. Tests
- Add tests verifying:
  - Generator creates non-empty `bom_by_name.csv` from a number-based BOM.
  - Import using name-based BOM produces relationships.
  - Empty or reversed-column name BOMs are handled.

6. Docs
- Update `IMPORTING.md` with regeneration behavior, env var, CLI flags, and troubleshooting for empty name BOM.

## Workplan
- Implement generator and parser improvements.
- Update launch script regeneration conditions and FORCE flag.
- Add tests and docs.

## Confirmation
Confirm you want these changes; once approved, I’ll implement, run tests, and verify import creates relationships using the name-based BOM.