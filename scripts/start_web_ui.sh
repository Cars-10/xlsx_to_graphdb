#!/usr/bin/env bash
# Start the Spreadsheet → GraphDB Importer web UI

set -euo pipefail

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Virtual environment not found. Creating one..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install/upgrade dependencies
echo "Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt

# Start the web server
echo ""
echo "=========================================="
echo "Windchill XLSX to Knowledge Graph"
echo "=========================================="
echo ""
echo "Web UI will be available at:"
echo "  http://localhost:5050"
echo ""
echo "Make sure your database is running:"
echo "  - GraphDB at http://localhost:7200"
echo "  - Neo4j at http://localhost:7474"
echo ""
echo "Press Ctrl+C to stop the server"
echo "=========================================="
echo ""

python src/web_server.py
