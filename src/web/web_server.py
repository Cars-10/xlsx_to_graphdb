#!/usr/bin/env python3
"""
Flask web server for the Spreadsheet → GraphDB Importer web UI.
Provides REST API endpoints for file listing, repository discovery, and import execution.
"""

import os
import sys
import json
import subprocess
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from flask import Flask, request, jsonify, send_from_directory, make_response
from flask_cors import CORS

# Add src directory to path for importing spreadsheet_loader
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import spreadsheet_loader

app = Flask(__name__)
CORS(app)  # Enable CORS for development

# Disable caching for development
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'data'


@app.after_request
def add_no_cache_headers(response):
    """Add no-cache headers to all responses during development."""
    if request.path == '/' or request.path.endswith('.html'):
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '-1'
    return response


@app.route('/')
def index():
    """Serve the main UI page with cache-busting headers."""
    response = make_response(send_from_directory(os.path.dirname(__file__), 'web_ui.html'))
    # Disable all caching for HTML file
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response


@app.route('/api/excel-files', methods=['GET'])
def list_excel_files():
    """List all Excel files in the data directory."""
    try:
        excel_files = [
            f.name for f in DATA_DIR.glob('*.xlsx')
            if f.is_file() and not f.name.startswith('~')
        ]
        return jsonify(sorted(excel_files))
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# BOM files are regenerated automatically from Excel; endpoint removed


@app.route('/api/graphdb-repositories', methods=['GET'])
def list_graphdb_repositories():
    """List all repositories from GraphDB."""
    try:
        url = 'http://localhost:7200/repositories'
        req = Request(url)
        req.add_header('Accept', 'application/json')

        with urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode())
            repositories = []

            # GraphDB returns SPARQL query results format
            # Structure: { "results": { "bindings": [ {...}, {...} ] } }
            if isinstance(data, dict) and 'results' in data and 'bindings' in data['results']:
                bindings = data['results']['bindings']
                for binding in bindings:
                    # Extract values from SPARQL result format
                    repo_id = binding.get('id', {}).get('value', '')
                    repo_title = binding.get('title', {}).get('value', '')
                    repo_uri = binding.get('uri', {}).get('value', '')

                    # Use ID as title if title is empty
                    if not repo_title:
                        repo_title = repo_id

                    repositories.append({
                        'id': repo_id,
                        'title': repo_title,
                        'uri': repo_uri
                    })
            elif isinstance(data, list):
                # Fallback: handle simple list format (in case API changes)
                for repo in data:
                    repositories.append({
                        'id': repo.get('id', ''),
                        'title': repo.get('title', repo.get('id', '')),
                        'uri': repo.get('uri', '')
                    })

            return jsonify(repositories)
    except URLError as e:
        # Return empty array instead of error object so UI can handle gracefully
        return jsonify([])
    except Exception as e:
        return jsonify([])


@app.route('/api/neo4j-databases', methods=['GET'])
def list_neo4j_databases():
    """List all databases from Neo4j."""
    try:
        # Neo4j's REST API for listing databases
        url = 'http://localhost:7474/db/neo4j/tx/commit'

        req = Request(url, method='POST')
        req.add_header('Content-Type', 'application/json')
        req.add_header('Accept', 'application/json')

        # Query to show databases
        query = {
            'statements': [
                {
                    'statement': 'SHOW DATABASES',
                    'resultDataContents': ['row']
                }
            ]
        }

        data = json.dumps(query).encode('utf-8')

        try:
            with urlopen(req, data, timeout=5) as response:
                result = json.loads(response.read().decode())
                databases = []

                # Extract database names from results
                if 'results' in result and len(result['results']) > 0:
                    for row in result['results'][0].get('data', []):
                        if 'row' in row and len(row['row']) > 0:
                            db_name = row['row'][0]
                            databases.append({
                                'id': db_name,
                                'name': db_name,
                                'title': db_name
                            })

                # If no databases found, provide defaults
                if not databases:
                    databases = [
                        {'id': 'neo4j', 'name': 'neo4j', 'title': 'neo4j (default)'},
                        {'id': 'system', 'name': 'system', 'title': 'system'}
                    ]

                return jsonify(databases)
        except HTTPError as e:
            # If authentication required or other HTTP error, return default databases
            databases = [
                {'id': 'neo4j', 'name': 'neo4j', 'title': 'neo4j (default)'},
                {'id': 'system', 'name': 'system', 'title': 'system'}
            ]
            return jsonify(databases)

    except URLError as e:
        # Return empty array instead of error object so UI can handle gracefully
        return jsonify([])
    except Exception as e:
        return jsonify([])


@app.route('/api/import', methods=['POST'])
def run_import():
    """Execute the import process with the provided configuration."""
    try:
        config = request.json
        if not config:
            return jsonify({'error': 'Invalid JSON data'}), 400

        # Validate required fields
        if not config.get('excelFile'):
            return jsonify({'error': 'Excel file is required'}), 400
        if not config.get('databases') or len(config['databases']) == 0:
            return jsonify({'error': 'At least one target database is required'}), 400

        # Build file paths
        excel_path = str(DATA_DIR / config['excelFile'])

        # Always regenerate BOM files from the Excel workbook
        excel_stem = Path(config['excelFile']).stem
        bom_parent_child = DATA_DIR / f"{excel_stem}_bom_parent_child.csv"
        bom_name = DATA_DIR / f"{excel_stem}_bom_by_name.csv"
        try:
            from spreadsheet_loader import generate_bom_from_excel
            generate_bom_from_excel(excel_path, str(bom_parent_child), str(bom_name))
            bom_path = str(bom_name)
            config['bomByName'] = True
        except Exception as e:
            return jsonify({'error': f'Failed to regenerate BOM from Excel: {str(e)}'}), 500

        # Validate files exist
        if not os.path.exists(excel_path):
            return jsonify({'error': f'Excel file not found: {config["excelFile"]}'}), 404
        if bom_path and not os.path.exists(bom_path):
            return jsonify({'error': f'BOM file not generated: {bom_path}'}), 500

        results = []

        # Execute import for each selected database
        for db_config in config['databases']:
            db_type = db_config['type']
            repository = db_config['repository']

            # Determine database URL
            if db_type == 'graphdb':
                db_url = 'http://localhost:7200'
            elif db_type == 'neo4j':
                db_url = 'bolt://localhost:7687'  # Use bolt protocol for Neo4j
            else:
                continue

            # Build command arguments
            cmd = [
                sys.executable,
                str(PROJECT_ROOT / 'src' / 'spreadsheet_loader.py'),
                '--excel', excel_path,
                '--url', db_url,
                '--repo', repository
            ]

            # Add optional BOM file
            if bom_path:
                cmd.extend(['--bom', bom_path])
                if config.get('bomByName'):
                    cmd.append('--bom-by-name')

            # Add batch size
            if config.get('batchSize'):
                cmd.extend(['--batch-size', str(config['batchSize'])])

            # Add log level
            if config.get('logLevel'):
                cmd.extend(['--log-level', config['logLevel']])

            # Add Neo4j credentials
            if config.get('neo4jUsername'):
                cmd.extend(['--user', config['neo4jUsername']])
            if config.get('neo4jPassword'):
                cmd.extend(['--password', config['neo4jPassword']])

            # Add boolean flags
            if config.get('strictNames'):
                cmd.append('--strict-names')
            if config.get('debugNames'):
                cmd.extend([
                    '--debug-names',
                    '--resolution-report', str(DATA_DIR / 'bom_name_resolution_report.csv'),
                    '--dump-name-index', str(DATA_DIR / 'name_index.csv'),
                    '--skip-log', str(DATA_DIR / 'skipped_names.log')
                ])
            if config.get('addEdgeLabels'):
                cmd.append('--add-edge-labels')
            if config.get('dryRun'):
                cmd.append('--dry-run')

            # Always add quiet-missing-sheets for cleaner output
            cmd.append('--quiet-missing-sheets')

            # Execute the import
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )

            result_info = {
                'database': db_type,
                'repository': repository,
                'success': result.returncode == 0,
                'output': result.stdout,
                'error': result.stderr
            }
            results.append(result_info)

        # Check if any import succeeded
        any_success = any(r['success'] for r in results)
        all_success = all(r['success'] for r in results)

        if all_success:
            message = f'Import completed successfully to {len(results)} database(s)!'
            if config.get('dryRun'):
                message = f'Dry run completed successfully (no data imported to {len(results)} database(s)).'

            return jsonify({
                'success': True,
                'message': message,
                'results': results
            })
        elif any_success:
            success_dbs = [r['database'] for r in results if r['success']]
            failed_dbs = [r['database'] for r in results if not r['success']]
            message = f'Partial success: imported to {", ".join(success_dbs)}. Failed: {", ".join(failed_dbs)}'

            return jsonify({
                'success': False,
                'message': message,
                'results': results
            }), 207  # Multi-Status
        else:
            return jsonify({
                'success': False,
                'error': f'Import failed to all {len(results)} database(s)',
                'results': results
            }), 500

    except subprocess.TimeoutExpired:
        return jsonify({'error': 'Import timed out after 5 minutes'}), 504
    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'version': '1.0.0',
        'project_root': str(PROJECT_ROOT),
        'data_dir': str(DATA_DIR)
    })


if __name__ == '__main__':
    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)

    print("=" * 60)
    print("Windchill XLSX to Knowledge Graph Web Server")
    print("=" * 60)
    print(f"Project root: {PROJECT_ROOT}")
    print(f"Data directory: {DATA_DIR}")
    print()
    print("Starting server at http://localhost:5050")
    print("Press Ctrl+C to stop")
    print("=" * 60)

    app.run(host='0.0.0.0', port=5050, debug=True)
