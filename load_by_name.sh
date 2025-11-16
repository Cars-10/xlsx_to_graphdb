#!/usr/bin/env bash
set -euo pipefail
DEBUG_NAMES=1
EXCEL="${EXCEL_PATH:-Snowmobile.xlsx}"
BOM="${BOM_PATH:-bom_by_name.csv}"
URL="${GRAPHDB_URL:-http://127.0.0.1:7200}"
REPO="${GRAPHDB_REPO:-Snowmobile}"
USER="${GRAPHDB_USER:-}"
PASS="${GRAPHDB_PASS:-}"
BATCH_SIZE="${BATCH_SIZE:-}"
STRICT_NAMES="${STRICT_NAMES:-}"
FORCE_GENERATE="${FORCE_GENERATE_BOM_BY_NAME:-}"
DEBUG_NAMES="${DEBUG_NAMES:-}"
SKIP_LOG="${SKIP_LOG:-skipped_names.log}"
ARGS=(snowmobile_importer.py --excel "$EXCEL" --url "$URL" --repo "$REPO" --bom-by-name --add-edge-labels --debug-names --quiet-missing-sheets)
regen_needed=0
if [ -n "$FORCE_GENERATE" ]; then regen_needed=1; fi
if [ -f "$BOM" ]; then
  lines=$(wc -l < "$BOM" | tr -d ' ')
  if [ "$lines" -le 1 ]; then regen_needed=1; fi
fi
if [ "$regen_needed" -eq 1 ]; then
  if [ -f "bom.csv" ]; then
    echo "Generating name-based BOM from bom.csv"
    python snowmobile_importer.py --excel "$EXCEL" --bom "bom.csv" --generate-bom-by-name --out-bom-name "$BOM"
  else
    echo "Warning: no number-based BOM found to generate name-based BOM" >&2
  fi
fi
if [ -f "$BOM" ]; then
  echo "Using name-based BOM: $BOM"
  ARGS+=(--bom "$BOM" --bom-by-name)
else
  if [ -f "bom.csv" ]; then
    echo "Name-based BOM not found; falling back to number-based BOM: bom.csv"
    ARGS+=(--bom "bom.csv")
  else
    echo "Warning: no BOM CSV found; proceeding without relationships" >&2
  fi
fi
if [ -n "$USER" ] && [ -n "$PASS" ]; then
  ARGS+=(--user "$USER" --password "$PASS")
fi
if [ -n "$BATCH_SIZE" ]; then
  ARGS+=(--batch-size "$BATCH_SIZE")
fi
if [ -n "$STRICT_NAMES" ] && [ -f "$BOM" ]; then
  ARGS+=(--strict-names)
fi
if [ -n "$DEBUG_NAMES" ]; then
  # Write reports to default filenames in CWD
  ARGS+=(--debug-names --resolution-report "bom_name_resolution_report.csv" --dump-name-index "name_index.csv" --skip-log "$SKIP_LOG")
fi
python "${ARGS[@]}"
