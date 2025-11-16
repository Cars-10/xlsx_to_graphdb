# Windchill XLSX to Knowledge Graph

## Why Use This Tool
- Load Windchill-style parts and BOM data from Excel workbooks into a graph database quickly and repeatably.
- See part relationships visually with friendly node names (labels), enabling faster understanding of assemblies and components.
- Automate and validate the import with batching, logging, error handling, and optional reports for diagnosis.

## What You Can Do
- Parse Excel workbooks (e.g., `Snowmobile.xlsx`, `Helicopter.xlsx`, `Mower.xlsx`) and build a number→name index.
- Import parts into GraphDB with readable labels (`rdfs:label` and `urn:ontology:name`).
- Create parent→child relationships using:
  - Number-based BOM (`data/bom.csv`) for reliability, or
  - Name-based BOM (`data/bom_by_name.csv`) for human-friendly linkage.
- Generate name-based BOM from numbers and produce diagnostic reports:
  - `data/bom_by_name.csv` (generated from `data/bom.csv`)
  - `data/name_index.csv` (all known part numbers and names)
  - `data/bom_name_resolution_report.csv` (per-edge resolution status)
  - `data/skipped_names.log` (one line per unknown/ambiguous)
- Handle large datasets efficiently with batching and deduplication.
- Suppress noisy Excel style warnings and track progress clearly.

## Quick Start
- Load by names (with auto-generation if needed):
  - `bash scripts/load_by_name.sh`
- Load by numbers (reliable, still shows names in the UI):
  - `python src/spreadsheet_loader.py --excel data/Snowmobile.xlsx --bom data/bom.csv --url http://127.0.0.1:7200 --repo Snowmobile --quiet-missing-sheets`
- Enable debugging outputs:
  - `DEBUG_NAMES=1 bash scripts/load_by_name.sh`

## Key Behaviors
- Nodes are parts with URIs `urn:part:{number}` and labels set to the part name.
- Relationships use `urn:ontology:hasComponent` for parent→child.
- Name-based import resolves names to numbers; strict mode fails fast when names are unknown or ambiguous.
- Number-based import avoids name resolution and is recommended when onboarding data.

## Configuration
- URL and repository: `--url` (defaults to `http://127.0.0.1:7200`) and `--repo` (defaults to `Snowmobile`).
- Auth: `--user` and `--password` for Basic Auth if your GraphDB requires it.
- Batching: `--batch-size` controls triples per POST.
- Debugging: `--debug-names`, `--resolution-report`, `--dump-name-index`, `--skip-log`.
- Quiet sheets: `--quiet-missing-sheets` to suppress non-critical sheet warnings.

## Data Expectations
- Parts tabs include `Number` and `Name` (header may be duplicated in first data row; importer normalizes).
- BOM by numbers: `data/bom.csv` with `Number` (parent) and `Component Id` (child).
- BOM by names: `data/bom_by_name.csv` with `Parent Name` and `Child Name`.

## Troubleshooting
- Nothing loaded: verify `--repo Snowmobile` and avoid `--strict-names` until names are complete.
- Many unknown names in name-based import: prefer number-based import or generate candidates, edit names, and re-import.
- Labels not visible: Workbench usually shows `rdfs:label` automatically; ensure nodes were imported.

## Repo Hygiene
- `.gitignore` excludes virtual envs, caches, logs, and generated CSVs.
- `.gitattributes` marks Excel files as binary to avoid text diffs.

## Use Cases
- Visualize assemblies/components.
- Prototype knowledge graphs from PLM exports.
- Validate data quality and naming via resolution reports.

## Directory Structure
```
.
├── src/                          # Source code
│   └── spreadsheet_loader.py    # Main importer module
├── scripts/                      # Utility scripts
│   └── load_by_name.sh          # Automated import script
├── data/                         # Data files (Excel, CSV, reports)
├── docs/                         # Documentation
└── tests/                        # Unit tests
```