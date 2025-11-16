## Goal
Remove noisy sheet warnings and correct BOM generation logging so the output is clear and accurate.

## Changes
1. Missing sheet warnings
- Add `warn_missing_required` to `SpreadsheetParser` and a CLI flag `--quiet-missing-sheets`.
- When quiet, do not emit WARNINGs for sheets missing `Number` and `Name`; log at DEBUG or skip logging.
- Wire `import_data(..., quiet_missing_sheets)` to pass through.

2. BOM generation counters
- In `generate_bom_by_name_file`, report `written` and `unmapped` (count of fallbacks to numbers), not `skipped` (since we don’t drop rows when falling back).
- Update log message accordingly.

3. Launch script
- Add `--quiet-missing-sheets` to `load_by_name.sh` invocation to reduce noise in typical runs.

## Tests
- No functional change; existing tests remain valid. Logging severity changes do not affect test outcomes.

## Docs
- Optional: note `--quiet-missing-sheets` in `IMPORTING.md`.

## After Implementation
- Running with `--quiet-missing-sheets` will eliminate the “WTPartAlternateLink-Sheet missing required columns” warnings.
- BOM generation logs will accurately reflect rows written and fallbacks used.

## Proceeding
I will implement these changes and verify tests still pass.