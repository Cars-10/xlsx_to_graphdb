## Goal
Add a number→name index of parts and support building relationships using part names. Resolve names back to numbers for stable URIs, so nodes still use part numbers but links can be specified by names.

## Implementation Changes
1. Index builder
- Create `build_name_index(parts)` returning two dicts: `pn_to_name` and `name_to_pn` (unique names only).
- Normalize names via `str(name).strip()`.

2. BOM parsing by name
- Add `SpreadsheetParser.parse_bom_csv_by_name(path)` reading columns `Parent Name`/`Child Name` or `Name`/`Component Name`.
- Return `List[Tuple[str, str]]` of `(parent_name, child_name)`.

3. Name→number resolution
- Add `resolve_edges_by_name(name_edges, name_to_pn)` that maps names to numbers, logs and skips unknown or ambiguous names.

4. Import pipeline integration
- In `import_data`, after `parts = parse_parts(...)`:
  - Build `pn_to_name`, `name_to_pn`.
  - If `--bom-by-name` set, parse name-based BOM and resolve to number edges, then merge with number-based edges.
- Continue to `build_bom_triples` using resolved numbers.

5. CLI flags
- Add `--bom-by-name` (bool) to enable name-based BOM parsing.
- Add `--strict-names` to fail on unknown/ambiguous names; otherwise warn and skip.

6. Logging & validation
- Warn on duplicate names; if duplicates exist, resolve based on `--strict-names` (fail) or skip ambiguous.
- Log counts: parts, number-based edges, name-based edges, skipped due to unknown/ambiguous.

## Data Flow
- Parts: Excel → `parts` dict → index (`pn_to_name`, `name_to_pn`) → node triples.
- BOM: CSV → number edges OR name edges → resolve via index → combined edges → relationship triples.

## Edge Cases
- Duplicate part names: detect; optionally fail with `--strict-names` or skip.
- Missing names: fall back to part number as name in index; still resolvable.
- Mixed BOM CSV formats: auto-detect columns; prefer explicit `--bom-by-name`.

## Tests
- Add tests:
  - Name index creation and duplicate detection.
  - Name-based BOM parsing with `Parent Name`/`Child Name`.
  - Resolution of names to numbers; unknown/ambiguous skipped vs strict mode failure.
  - Integration: total triple counts include name-linked relationship.

## Documentation
- Update `IMPORTING.md` with name-based BOM format, CLI flags, duplicate name handling, and troubleshooting for unknown names.

## Request for Confirmation
Confirm you want name-based BOM linking using the index and the above flags. After confirmation, I will implement the changes, update tests and docs, and run the test suite.