#!/usr/bin/env bash
# Quick-start script for loading data into Neo4j with stunning visualizations

set -euo pipefail

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Configuration (can be overridden by environment variables)
EXCEL_PATH="${EXCEL_PATH:-data/Snowmobile.xlsx}"
BOM_PATH="${BOM_PATH:-data/bom.csv}"
NEO4J_URI="${NEO4J_URI:-bolt://localhost:7687}"
NEO4J_DATABASE="${NEO4J_DATABASE:-neo4j}"
NEO4J_USER="${NEO4J_USER:-neo4j}"
NEO4J_PASS="${NEO4J_PASS:-password}"
BATCH_SIZE="${BATCH_SIZE:-1000}"

echo "=============================================="
echo "Windchill XLSX to Neo4j Knowledge Graph"
echo "=============================================="
echo ""
echo "Configuration:"
echo "  Excel file:    $EXCEL_PATH"
echo "  BOM file:      $BOM_PATH"
echo "  Neo4j URI:     $NEO4J_URI"
echo "  Database:      $NEO4J_DATABASE"
echo "  Batch size:    $BATCH_SIZE"
echo ""
echo "Starting import..."
echo "=============================================="
echo ""

# Build command
cmd=(
    python src/spreadsheet_loader.py
    --excel "$EXCEL_PATH"
    --bom "$BOM_PATH"
    --url "$NEO4J_URI"
    --repo "$NEO4J_DATABASE"
    --user "$NEO4J_USER"
    --password "$NEO4J_PASS"
    --batch-size "$BATCH_SIZE"
    --quiet-missing-sheets
)

# Execute import
"${cmd[@]}"

echo ""
echo "=============================================="
echo "Import complete!"
echo ""
echo "Next steps:"
echo "  1. Open Neo4j Browser: http://localhost:7474"
echo "  2. Login with username: $NEO4J_USER"
echo "  3. Try this query to see your data:"
echo ""
echo "     MATCH (p:Part)-[r:HAS_COMPONENT]->(child)"
echo "     RETURN p, r, child"
echo "     LIMIT 50"
echo ""
echo "  4. Customize visualization:"
echo "     - Click 'Part' in bottom panel"
echo "     - Set Caption: name"
echo "     - Set Color by: displayColor"
echo "     - Set Size by: size"
echo ""
echo "For more visualization tips, see:"
echo "  docs/NEO4J_VISUALIZATION.md"
echo "=============================================="
