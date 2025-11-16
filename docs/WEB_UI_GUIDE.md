# Web UI Guide

## Overview
The Spreadsheet → GraphDB Importer now includes a modern web interface that makes importing data easy and intuitive.

## Starting the Web UI

### Quick Start
```bash
bash scripts/start_web_ui.sh
```

This script will:
1. Create a virtual environment (if needed)
2. Install all dependencies
3. Start the web server at http://localhost:5050

### Manual Start
```bash
# Install dependencies
pip install -r requirements.txt

# Start server
python src/web_server.py
```

## Using the Web Interface

### 1. Data Source Section
**Excel File**: Dropdown showing all `.xlsx` files in the `data/` directory
- Automatically detects Snowmobile.xlsx, Helicopter.xlsx, Mower.xlsx, etc.
- Hover over the `?` icon for detailed descriptions

**BOM File**: Optional dropdown for Bill of Materials CSV files
- Shows all CSV files with "bom" in the name
- Select "None" to import parts only (no relationships)

**BOM uses names**: Checkbox to enable name-based BOM parsing
- Use for files with "Parent Name" and "Child Name" columns
- System will automatically resolve names to part numbers

### 2. Target Database Section
**Database Selector**: Choose between GraphDB or Neo4j
- GraphDB: http://localhost:7200 (RDF triple store)
- Neo4j: http://localhost:7474 (Property graph - coming soon)

**Repository/Database**: Dropdown populated automatically
- For GraphDB: Lists all available repositories
- For Neo4j: Lists all available databases
- Click "Refresh Lists" if you create a new repository

**Authentication**: Optional username and password fields
- Only needed if your database requires authentication
- Credentials are sent securely to the backend

### 3. Advanced Options Section
All options include helpful tooltips when you hover over the `?` icon:

**Batch Size** (default: 1000)
- Controls how many RDF triples are sent per POST request
- Lower values = less memory, more network calls
- Higher values = faster import, more memory usage

**Strict name resolution** (checkbox)
- When enabled: Import fails if unknown/ambiguous names are found
- When disabled: Unknown/ambiguous names are skipped and logged
- Useful for validating data quality

**Generate debug reports** (checkbox)
- Creates diagnostic files in `data/`:
  - `name_index.csv` - All part number → name mappings
  - `bom_name_resolution_report.csv` - Per-edge resolution status
  - `skipped_names.log` - List of skipped entries with reasons

**Add edge labels** (checkbox)
- Adds `rdfs:label` to relationship edges
- Improves visualization in graph browsers
- Slightly increases import time

**Dry run** (checkbox)
- Process data and generate reports WITHOUT importing
- Perfect for testing and validation
- Shows what would be imported

### 4. Action Buttons
**Refresh Lists**: Reloads Excel files, BOM files, and repositories
- Use after adding new files to `data/` directory
- Use after creating new repositories

**Start Import**: Begins the import process
- Button shows loading animation during import
- Status panel displays progress and results

## Status Messages
The interface shows three types of status messages:

- **Blue (Info)**: Import is starting or in progress
- **Green (Success)**: Import completed successfully
- **Red (Error)**: Import failed with error details

## Features

### Automatic Discovery
- Excel files are automatically detected from `data/` directory
- BOM files are auto-filtered to show only relevant CSVs
- Repositories are fetched live from GraphDB/Neo4j

### Interactive Tooltips
Every option has a `?` icon that shows detailed help when you hover:
- Explains what the option does
- Provides recommendations
- Shows default values

### Visual Feedback
- Database selector highlights your choice
- Form validation ensures required fields are filled
- Loading animations show when operations are in progress
- Color-coded status messages

### Responsive Design
- Works on desktop, tablet, and mobile
- Clean, modern interface with gradient design
- Smooth transitions and animations

## API Endpoints
The web server provides these REST endpoints:

- `GET /api/excel-files` - List Excel files in data/
- `GET /api/bom-files` - List BOM CSV files
- `GET /api/graphdb-repositories` - List GraphDB repositories
- `GET /api/neo4j-databases` - List Neo4j databases
- `POST /api/import` - Execute import with configuration
- `GET /api/health` - Health check

## Troubleshooting

### "Cannot connect to GraphDB at localhost:7200"
- Make sure GraphDB is running
- Check that it's accessible at http://localhost:7200
- Verify no firewall is blocking the connection

### "Cannot connect to Neo4j at localhost:7474"
- Make sure Neo4j is running
- Check that it's accessible at http://localhost:7474
- Neo4j import functionality is coming soon

### "Excel file not found"
- Make sure the file exists in the `data/` directory
- Click "Refresh Lists" to reload available files
- Check file permissions

### "Import timed out"
- Large imports may take more than 5 minutes
- Consider using the command-line interface for very large datasets
- Increase batch size to speed up imports

### No repositories showing
- Click "Refresh Lists" button
- Verify database is running
- Check database authentication settings

## Best Practices

1. **Start with a dry run**: Use the "Dry run" option first to validate your data
2. **Enable debug reports**: Helps diagnose issues with name resolution
3. **Use number-based BOMs**: More reliable than name-based for production imports
4. **Test authentication**: Use the health endpoint to verify connectivity
5. **Monitor the logs**: Server console shows detailed import progress

## Security Notes
- The web server runs locally on localhost only
- Credentials are never logged or stored
- All communication happens between localhost services
- For production use, consider adding HTTPS and authentication
