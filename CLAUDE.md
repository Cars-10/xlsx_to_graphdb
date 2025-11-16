# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This repository is a Windchill demo data importer that extracts Bill of Materials (BOM) data from Excel spreadsheets and loads it into a GraphDB RDF triple store. The primary data source is `Snowmobile.xlsx`, which contains multiple tabs representing different part types and relationships.

## Core Workflow

The system follows a three-stage data transformation pipeline:

1. **Excel Parsing** → Extract parts from multiple sheets with normalized headers
2. **Name Resolution** → Build cross-reference indexes mapping part numbers to names
3. **Graph Import** → Convert to RDF triples and POST to GraphDB in batches

## Key Components

### Main Importer (`snowmobile_importer.py`)

The primary script with two main classes:

- **SpreadsheetParser**: Handles Excel parsing with automatic header normalization
  - Detects and normalizes duplicated header rows (first data row repeating column names)
  - Parses BOM relationships from CSV files (both number-based and name-based)
  - Builds cross-reference indexes from all sheets containing `Number` and `Name` columns

- **GraphDBClient**: Manages GraphDB connection and RDF data posting
  - Verifies repository existence before importing
  - Posts RDF triples in configurable batches (default 1000)
  - Supports Basic Authentication

### Data Model

- **Part Numbers vs. Names**: The system handles both number-based (e.g., "100 SNOW", "0114032") and name-based part references
- **Name Resolution**: Critical feature that maps part names to numbers using a cross-index built from all sheets
  - Handles ambiguous names (multiple parts with same name)
  - Handles unknown names (names not found in index)
  - Provides detailed diagnostics via `--debug-names` and `--resolution-report`

### RDF Schema

- Namespace: `urn:ontology`
- Part URIs: `urn:part:{url-encoded-part-number}`
- Node type: `rdf:type` → `urn:ontology:Part`
- Node labels: `urn:ontology:name` and `rdfs:label`
- Relationships: `urn:ontology:hasComponent` (parent → child)

## Excel Structure

The `Snowmobile.xlsx` workbook contains these sheets:
- `MechanicalPart-Sheet`
- `Variant-Sheet`
- `WTPart-Sheet`
- `Snowmobile-Sheet`
- `SoftwarePart-Sheet`
- `BOMSheet1`
- `WTPartAlternateLink-Sheet`

Each sheet may have:
- Duplicated header pattern: First data row repeats column names
- Required columns: `Number`, `Name`
- Optional columns: `Type`, `Source`, `Revision`, `View`, `Container`

The parser automatically normalizes headers by detecting when the first data row contains the column names.

## BOM CSV Formats

### Number-based BOM (`bom.csv`)
Contains these column pairs (auto-detected):
- `Number` + `Component Id`, OR
- `Parent Number` + `Child Number`

### Name-based BOM (`bom_by_name.csv`)
Contains these column pairs (auto-detected):
- `Parent Name` + `Child Name`, OR
- `Name` + `Component Name`

## Common Commands

### Virtual Environment Setup
```bash
python3 -m venv venv
source venv/bin/activate
pip install pandas openpyxl rdflib
```

### Basic Import (Number-based)
```bash
python snowmobile_importer.py \
  --excel Snowmobile.xlsx \
  --bom bom.csv \
  --url http://127.0.0.1:7200 \
  --repo Snowmobile
```

### Name-based Import with Full Diagnostics
```bash
./load_by_name.sh
```

This script automatically:
- Generates `bom_by_name.csv` from `bom.csv` if needed
- Enables debug output (`--debug-names`)
- Creates resolution report (`bom_name_resolution_report.csv`)
- Dumps name index (`name_index.csv`)
- Logs skipped entries (`skipped_names.log`)

### Manual Name-based BOM Generation
```bash
python snowmobile_importer.py \
  --excel Snowmobile.xlsx \
  --bom bom.csv \
  --generate-bom-by-name \
  --out-bom-name bom_by_name.csv
```

### Dump Name Index (for debugging)
```bash
python snowmobile_importer.py \
  --excel Snowmobile.xlsx \
  --dump-name-index name_index.csv
```

### Dry Run (test without importing)
```bash
python snowmobile_importer.py \
  --excel Snowmobile.xlsx \
  --bom bom.csv \
  --dry-run
```

## Key Command-Line Options

- `--bom-by-name`: Parse BOM CSV using names instead of numbers
- `--strict-names`: Fail on unknown/ambiguous names (default: skip)
- `--debug-names`: Enable detailed name resolution diagnostics
- `--resolution-report PATH`: Write CSV report of name resolution results
- `--skip-log PATH`: Log file for skipped name-based edges
- `--dump-name-index PATH`: Export the number→name cross-reference
- `--quiet-missing-sheets`: Suppress warnings for sheets without required columns
- `--batch-size N`: Control triples per POST (default 1000)
- `--sheets SHEET1 SHEET2`: Restrict parsing to specific sheets

## Environment Variables (for load_by_name.sh)

- `EXCEL_PATH`: Path to Excel file (default: `Snowmobile.xlsx`)
- `BOM_PATH`: Path to BOM CSV (default: `bom_by_name.csv`)
- `GRAPHDB_URL`: GraphDB URL (default: `http://127.0.0.1:7200`)
- `GRAPHDB_REPO`: Repository name (default: `Snowmobile`)
- `GRAPHDB_USER`: Username for authentication
- `GRAPHDB_PASS`: Password for authentication
- `BATCH_SIZE`: Batch size override
- `STRICT_NAMES`: Set to enable strict name resolution
- `FORCE_GENERATE_BOM_BY_NAME`: Force regeneration of name-based BOM
- `SKIP_LOG`: Path for skipped entries log (default: `skipped_names.log`)

## Utility Scripts

- `read_excel.py`: Print all sheet names from Excel file
- `read_sheet.py`: Export a specific sheet as CSV (skips first 4 rows)
- `extract_parts.py`: Extract parts from specified sheets as JSON
- `generate_load_script.py`: Generate bash script with curl commands (legacy approach)

## Important Implementation Details

### Header Normalization Logic
The parser checks if the first data row contains column names by comparing the first row values against expected columns (`Number`, `Name`). If detected, it promotes that row to be the column headers and drops it from the data.

### Name Resolution Process
1. Build cross-index from all sheets with `Number` and `Name` columns
2. Create reverse mapping from names to part numbers
3. For each BOM edge (parent name, child name):
   - Look up candidates for both names
   - If exactly one candidate each: resolve to edge
   - If zero candidates: mark as "unknown"
   - If multiple candidates: mark as "ambiguous"
4. Skip unknown/ambiguous edges (unless `--strict-names` is set)

### Batch Processing
RDF triples are accumulated and posted in configurable batches to handle large datasets efficiently. The default batch size of 1000 triples balances memory usage and network overhead.

## Troubleshooting

- **Import fails with connection errors**: Verify GraphDB is running at the configured URL and the repository exists
- **Name resolution has many skipped edges**: Check `skipped_names.log` and `bom_name_resolution_report.csv` to identify unknown or ambiguous names
- **Missing parts in graph**: Ensure the sheet contains `Number` and `Name` columns; check logs for sheet parsing warnings
- **Duplicate name warnings**: Use `--strict-names` to fail fast, or review the resolution report to understand which names are ambiguous
