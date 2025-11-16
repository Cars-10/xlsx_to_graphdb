# Snowmobile Excel → GraphDB Importer

## Required Spreadsheet Format
- Workbook: `Snowmobile.xlsx` (case-insensitive; override with `--excel`)
- Sheets: Each sheet may contain the following columns with a duplicated header row pattern:
  - `Number` (required)
  - `Name` (required)
  - `Type` (optional)
  - `Source` (optional)
- Header duplication: The first data row repeats the column names; the importer normalizes this automatically.
- BOM relationships (optional): Provide a CSV via `--bom` with columns either `Number,Component Id` or `Parent Number,Child Number` to define parent→child component links.
 - Alternate link and BOM tabs: The importer will also scan tabs like `WTPartAlternateLink-Sheet` and `BOMSheet1` to enrich the Number→Name index using any sheet that includes both `Number` and `Name`. Numbers found in `Component Id`, `Parent Part Number`, `Child Part Number`, and `Replacement Part Number` are mapped to names via the index.

## Expected Graph Schema
- Namespace: `urn:ontology`
- Part node URIs: `urn:part:{url-encoded part_number}`
- Node type: `rdf:type` → `urn:ontology:Part`
- Node label: `urn:ontology:name` → literal part name
- Relationship edges: `urn:ontology:hasComponent` from parent part to child part

## Configuration Requirements
- GraphDB URL: `--url` (default `http://127.0.0.1:7200`)
- Repository id: `--repo` (default `Snowmobile`)
- Authentication: `--user` and `--password` for Basic Auth if enabled; credentials are never logged.
- Batching: `--batch-size` controls triples per POST (default 1000) to handle large datasets efficiently.
- Sheets: `--sheets` restricts parsing to specific sheet names.
- Dry-run: `--dry-run` prepares data without POSTing to GraphDB.
- Name-based BOM: `--bom-by-name` parses BOM CSV using names instead of numbers; `--strict-names` fails on unknown or ambiguous names.
- Name-based BOM generation: `--generate-bom-by-name` creates `bom_by_name.csv` from `bom.csv`. Use `--out-bom-name` to change output.
- Script regeneration: Set `FORCE_GENERATE_BOM_BY_NAME=1` to force rebuilding `bom_by_name.csv` in `load_by_name.sh`. The script also regenerates when the file is empty (≤1 line).
 - Quiet sheets: `--quiet-missing-sheets` suppresses warnings for tabs that do not contain required `Number` and `Name` columns.

## Troubleshooting
- Connection failed / repository not found:
  - Verify GraphDB is running and accessible at `--url`.
  - Check that repository `--repo` exists; visit `{url}/workbench` and create the repository if needed.
- Missing columns:
  - Ensure sheets include `Number` and `Name` columns; the importer skips sheets lacking required columns.
- Empty import:
  - Confirm the workbook path and that sheets are not empty after header normalization.
- Authentication errors:
  - Provide correct `--user` and `--password`; ensure the account has write access to the repository.
- Large dataset performance:
  - Increase `--batch-size` to reduce POST requests; monitor memory usage when batching.
- Duplicate entries:
  - Nodes are keyed by part number URIs; duplicates in the same run are deduplicated and GraphDB handles repeated statements gracefully.
- Duplicate names:
  - If multiple parts share the same name, name-based resolution is ambiguous. Use `--strict-names` to fail fast, or keep the default to skip ambiguous edges.

## Usage
- Command-line:
  - `python snowmobile_importer.py --excel Snowmobile.xlsx --bom bom.csv --url http://127.0.0.1:7200 --repo Snowmobile`
  - Add `--user USER --password PASS` if authentication is enabled.
  - Name-based BOM: `python snowmobile_importer.py --excel Snowmobile.xlsx --bom bom_by_name.csv --bom-by-name --repo Snowmobile`
- Programmatic:
  - Create `GraphDBClient(url, repo, user, password)` and call `import_data(excel_path, bom_csv_path, client, batch_size, dry_run)`.
  - Name-based: pass `bom_by_name=True` and `strict_names=True|False`.
  - Generate name-based: call `generate_bom_by_name_file(excel_path, bom_csv_path, out_path)`.