## Goal
Provide detailed diagnostics for why name-based BOM edges are skipped and generate intermediate files to help resolve unknown or ambiguous names.

## Debugging Additions
- Enable verbose logging for name resolution:
  - Log each skipped edge with parent/child names, normalized forms, and resolution reason (unknown/ambiguous).
  - Log candidate part numbers for ambiguous names.
- Add a summary breakdown after resolution:
  - `resolved_count`, `ambiguous_resolved_count` (if non-strict tie-breakers are used), `unknown_count`, `ambiguous_count`.

## Intermediate Files
1. Name Index Dump
- `name_index.csv`: full cross-sheet index of `part_number,name,source_sheet,revision,view,container`.
2. Candidate Resolution Report
- `bom_name_resolution_report.csv`: per BOM-by-name row:
  - `parent_name,child_name,parent_candidates,child_candidates,chosen_parent,chosen_child,status,reason`
- Useful to see which names failed or were ambiguous and why.
3. Generated Candidates for Name-Based BOM
- `bom_by_name_candidates.csv`: generated from `bom.csv` showing `parent_number,parent_name,child_number,child_name`.
- Highlights where names are missing; can be manually edited to fix names before import.

## CLI Flags
- `--debug-names` to turn on detailed logging and write reports.
- `--resolution-report path` for `bom_name_resolution_report.csv`.
- `--dump-name-index path` for `name_index.csv`.
- `--emit-bom-name-candidates path` to write `bom_by_name_candidates.csv` from number-based BOM.

## Script Integration
- Update `load_by_name.sh` to pass `--debug-names` and emit reports when `DEBUG_NAMES=1` is set.

## Implementation Outline
- Extend resolver to collect diagnostics per edge; write CSV if flag present.
- Add functions:
  - `dump_name_index(excel_path, out_path, sheets)`
  - `emit_bom_name_candidates(excel_path, bom_csv_path, out_path, sheets)`
- Wire flags into `main()` and conditionally run dumps before import.

## Tests
- Add unit tests that:
  - Produce a small `name_index.csv` and validate contents.
  - Generate `bom_by_name_candidates.csv` from a sample `bom.csv`.
  - Write a `bom_name_resolution_report.csv` showing unknown/ambiguous entries.

## Outcome
- You will see exactly which names are failing and have editable intermediate files to correct them. If desired, we can later add auto tie-breakers to resolve ambiguities instead of skipping.

## Proceed
On approval, I will implement the flags, logging, and report generators, update the launch script optionally, and verify via tests.