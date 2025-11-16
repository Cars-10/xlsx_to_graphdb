## Why You See It
- During import with `--bom-by-name`, the pipeline still calls the number-based BOM parser (`parse_bom_csv`) on `bom_by_name.csv`.
- That parser expects columns like `Number/Component Id` or `Parent Number/Child Number`. When it sees `Parent Name/Child Name`, it logs the warning you’re seeing.
- Immediately after, the name-based parser (`parse_bom_csv_by_name`) runs and uses the file correctly. The warning is harmless noise.

## Changes To Silence It
1. Conditional parsing
- In `import_data`, skip `parse_bom_csv(bom_csv_path)` when `bom_by_name=True`.
- Only run `parse_bom_csv_by_name(bom_csv_path)` in that case.

2. Column auto-detect (optional)
- Detect headers on load and route to the correct parser automatically (no flag needed), avoiding warnings entirely.

3. Parser logging adjustment (optional)
- In `parse_bom_csv`, if name-based columns are present, return empty without logging a warning (or log at DEBUG).

## Tests
- Update tests that previously showed the warning to assert no warnings are emitted in name-based runs.

## Script Behavior
- `load_by_name.sh` remains the same; once the importer routes conditionally, the warning disappears.

## Unrelated Warnings
- `openpyxl` "default style" UserWarnings are unrelated and safe to ignore; can be suppressed if desired.

## Next Step
- I will implement conditional parsing in `import_data` and adjust logging to eliminate the warning, add a header auto-detect fallback, and update tests accordingly.