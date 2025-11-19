"""
Enhanced Flask web server with comprehensive error handling and validation.
Provides robust REST API endpoints with detailed error reporting.
"""

import os
import sys
import json
import subprocess
import time
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from typing import Dict, Any, Optional, Tuple

from flask import Flask, request, jsonify, send_from_directory, make_response
from flask_cors import CORS

# Add src directory to path for importing modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from validation import FileValidator, DatabaseValidator, ConfigurationValidator
from exceptions import (
    ValidationError, FileValidationError, DatabaseConnectionError,
    NetworkError, ConfigurationError, WindchillImporterError
)
from logging_config import setup_logging, get_logger, log_operation_start, log_operation_end

# Setup logging
setup_logging(level='INFO', include_console=True)
logger = get_logger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for development

# Disable caching for development
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

# Get project root directory
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / 'data'
SCRIPTS_DIR = PROJECT_ROOT / 'scripts'

# Configuration
DEFAULT_TIMEOUT = 30
MAX_FILE_SIZE_MB = 100


@app.errorhandler(ValidationError)
def handle_validation_error(error):
    """Handle validation errors with proper HTTP status codes."""
    logger.warning(f"Validation error: {error}", extra={
        'error_type': type(error).__name__,
        'field': getattr(error, 'field', None),
        'value': getattr(error, 'value', None)
    })
    
    return jsonify({
        'error': str(error),
        'error_type': 'validation_error',
        'field': getattr(error, 'field', None),
        'value': getattr(error, 'value', None)
    }), 400


@app.errorhandler(FileValidationError)
def handle_file_validation_error(error):
    """Handle file validation errors."""
    logger.error(f"File validation error: {error}", extra={
        'error_type': type(error).__name__,
        'field': getattr(error, 'field', None),
        'value': getattr(error, 'value', None)
    })
    
    return jsonify({
        'error': str(error),
        'error_type': 'file_validation_error',
        'field': getattr(error, 'field', None),
        'value': getattr(error, 'value', None)
    }), 400


@app.errorhandler(DatabaseConnectionError)
def handle_database_connection_error(error):
    """Handle database connection errors."""
    logger.error(f"Database connection error: {error}", extra={
        'error_type': type(error).__name__,
        'database_type': getattr(error, 'database_type', None),
        'url': getattr(error, 'url', None)
    })
    
    return jsonify({
        'error': str(error),
        'error_type': 'database_connection_error',
        'database_type': getattr(error, 'database_type', None),
        'url': getattr(error, 'url', None)
    }), 503


@app.errorhandler(NetworkError)
def handle_network_error(error):
    """Handle network errors."""
    logger.error(f"Network error: {error}", extra={
        'error_type': type(error).__name__,
        'url': getattr(error, 'url', None),
        'status_code': getattr(error, 'status_code', None)
    })
    
    return jsonify({
        'error': str(error),
        'error_type': 'network_error',
        'url': getattr(error, 'url', None),
        'status_code': getattr(error, 'status_code', None)
    }), 503


@app.errorhandler(Exception)
def handle_generic_error(error):
    """Handle unexpected errors."""
    logger.exception(f"Unexpected error: {error}")
    
    return jsonify({
        'error': 'An unexpected error occurred',
        'error_type': 'internal_error',
        'details': str(error) if app.debug else None
    }), 500


@app.after_request
def add_security_headers(response):
    """Add security headers to all responses."""
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    return response


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
    """List all Excel files in the data directory with validation."""
    log_operation_start("list_excel_files")
    start_time = time.time()
    
    try:
        # Validate data directory exists
        if not DATA_DIR.exists():
            raise FileValidationError(f"Data directory does not exist: {DATA_DIR}")
        
        if not DATA_DIR.is_dir():
            raise FileValidationError(f"Data path is not a directory: {DATA_DIR}")
        
        # List Excel files
        excel_files = []
        for f in DATA_DIR.glob('*.xlsx'):
            if f.is_file() and not f.name.startswith('~'):
                try:
                    # Quick validation of Excel files
                    FileValidator.validate_excel_file(f)
                    excel_files.append(f.name)
                    logger.debug(f"Validated Excel file: {f.name}")
                except FileValidationError as e:
                    logger.warning(f"Skipping invalid Excel file {f.name}: {e}")
        
        duration = time.time() - start_time
        log_operation_end("list_excel_files", success=True, duration=duration, files_count=len(excel_files))
        
        return jsonify(sorted(excel_files))
    
    except Exception as e:
        duration = time.time() - start_time
        log_operation_end("list_excel_files", success=False, duration=duration, error=str(e))
        raise


@app.route('/api/graphdb-repositories', methods=['GET'])
def list_graphdb_repositories():
    """List all repositories from GraphDB with enhanced error handling."""
    log_operation_start("list_graphdb_repositories")
    start_time = time.time()
    
    try:
        # Get and validate URL parameter
        base_url = request.args.get('url', 'http://localhost:7200')
        validated_url = DatabaseValidator.validate_graphdb_url(base_url)
        
        # Construct repositories endpoint
        url = f"{validated_url}/repositories"
        
        req = Request(url)
        req.add_header('Accept', 'application/json')
        
        with urlopen(req, timeout=DEFAULT_TIMEOUT) as response:
            data = json.loads(response.read().decode())
            repositories = []
            
            # Parse GraphDB response format
            if isinstance(data, dict) and 'results' in data and 'bindings' in data['results']:
                bindings = data['results']['bindings']
                for binding in bindings:
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
                # Fallback: handle simple list format
                for repo in data:
                    repositories.append({
                        'id': repo.get('id', ''),
                        'title': repo.get('title', repo.get('id', '')),
                        'uri': repo.get('uri', '')
                    })
            
            duration = time.time() - start_time
            log_operation_end("list_graphdb_repositories", success=True, duration=duration, 
                            repositories_count=len(repositories), url=validated_url)
            
            return jsonify(repositories)
    
    except URLError as e:
        logger.warning(f"GraphDB connection failed: {e}")
        # Return empty array instead of error for graceful UI handling
        duration = time.time() - start_time
        log_operation_end("list_graphdb_repositories", success=False, duration=duration, error=str(e))
        return jsonify([])
    
    except Exception as e:
        duration = time.time() - start_time
        log_operation_end("list_graphdb_repositories", success=False, duration=duration, error=str(e))
        raise


@app.route('/api/neo4j-databases', methods=['GET'])
def list_neo4j_databases():
    """List all databases from Neo4j with enhanced error handling."""
    log_operation_start("list_neo4j_databases")
    start_time = time.time()
    
    try:
        # Get and validate URL parameter
        base_url = request.args.get('url', 'http://localhost:7474')
        
        # Neo4j's REST API for listing databases
        url = f"{base_url}/db/neo4j/tx/commit"
        
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
            with urlopen(req, data, timeout=DEFAULT_TIMEOUT) as response:
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
                
                duration = time.time() - start_time
                log_operation_end("list_neo4j_databases", success=True, duration=duration,
                                databases_count=len(databases), url=base_url)
                
                return jsonify(databases)
        
        except HTTPError as e:
            # If authentication required or other HTTP error, return default databases
            logger.warning(f"Neo4j HTTP error, returning defaults: {e}")
            databases = [
                {'id': 'neo4j', 'name': 'neo4j', 'title': 'neo4j (default)'},
                {'id': 'system', 'name': 'system', 'title': 'system'}
            ]
            
            duration = time.time() - start_time
            log_operation_end("list_neo4j_databases", success=False, duration=duration, error=str(e))
            return jsonify(databases)
    
    except URLError as e:
        logger.warning(f"Neo4j connection failed: {e}")
        duration = time.time() - start_time
        log_operation_end("list_neo4j_databases", success=False, duration=duration, error=str(e))
        return jsonify([])
    
    except Exception as e:
        duration = time.time() - start_time
        log_operation_end("list_neo4j_databases", success=False, duration=duration, error=str(e))
        raise


@app.route('/api/import', methods=['POST'])
def run_import():
    """Execute the import process with comprehensive validation and error handling."""
    log_operation_start("run_import")
    start_time = time.time()
    
    try:
        # Validate request data
        config = request.json
        if not config:
            raise ValidationError("Invalid JSON data in request body")
        
        # Validate required fields
        if not config.get('excelFile'):
            raise ValidationError("Excel file is required", field="excelFile")
        
        if not config.get('databases') or len(config['databases']) == 0:
            raise ValidationError("At least one target database is required", field="databases")
        
        # Validate Excel file
        excel_filename = config['excelFile']
        excel_path = DATA_DIR / excel_filename
        
        try:
            FileValidator.validate_excel_file(excel_path)
        except FileValidationError as e:
            raise ValidationError(f"Invalid Excel file: {e}", field="excelFile", value=excel_filename)
        
        # Validate batch size if provided
        batch_size = config.get('batchSize', 1000)
        try:
            batch_size = ConfigurationValidator.validate_batch_size(batch_size)
        except ConfigurationError as e:
            raise ValidationError(f"Invalid batch size: {e}", field="batchSize", value=str(batch_size))
        
        # Validate timeout if provided
        timeout = config.get('timeout', DEFAULT_TIMEOUT)
        try:
            timeout = ConfigurationValidator.validate_timeout(timeout)
        except ConfigurationError as e:
            raise ValidationError(f"Invalid timeout: {e}", field="timeout", value=str(timeout))
        
        # Process each database
        results = []
        
        for db_config in config['databases']:
            try:
                db_type = db_config.get('type')
                if not db_type:
                    raise ValidationError("Database type is required", field="type")
                
                if db_type not in ['graphdb', 'neo4j']:
                    raise ValidationError(f"Unsupported database type: {db_type}", field="type", value=db_type)
                
                # Build command based on database type
                if db_type == 'graphdb':
                    result = _run_graphdb_import(excel_path, db_config, batch_size, timeout)
                else:  # neo4j
                    result = _run_neo4j_import(excel_path, db_config, batch_size, timeout)
                
                results.append(result)
            
            except Exception as e:
                logger.error(f"Import failed for database {db_config.get('name', 'unknown')}: {e}")
                results.append({
                    'database': db_config.get('name', 'unknown'),
                    'type': db_type,
                    'success': False,
                    'error': str(e)
                })
        
        # Check overall success
        overall_success = any(r.get('success', False) for r in results)
        
        duration = time.time() - start_time
        log_operation_end("run_import", success=overall_success, duration=duration,
                       databases_count=len(results), success_count=sum(1 for r in results if r.get('success', False)))
        
        return jsonify({
            'success': overall_success,
            'results': results,
            'duration': duration
        })
    
    except Exception as e:
        duration = time.time() - start_time
        log_operation_end("run_import", success=False, duration=duration, error=str(e))
        raise


def _run_graphdb_import(excel_path: Path, db_config: Dict[str, Any], batch_size: int, timeout: int) -> Dict[str, Any]:
    """Run GraphDB import with validation and error handling."""
    try:
        # Validate GraphDB configuration
        url = db_config.get('url', 'http://localhost:7200')
        validated_url = DatabaseValidator.validate_graphdb_url(url)
        
        repository = db_config.get('repository')
        if not repository:
            raise ValidationError("Repository name is required", field="repository")
        
        validated_repository = DatabaseValidator.validate_repository_name(repository)
        
        # Build command
        cmd = [
            sys.executable, str(SCRIPTS_DIR / 'load_by_name.sh'),
            '--excel', str(excel_path),
            '--url', validated_url,
            '--repo', validated_repository,
            '--batch-size', str(batch_size)
        ]
        
        logger.info(f"Running GraphDB import command: {' '.join(cmd)}")
        
        # Execute command with timeout
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=PROJECT_ROOT
        )
        
        success = result.returncode == 0
        
        return {
            'database': validated_repository,
            'type': 'graphdb',
            'success': success,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    
    except subprocess.TimeoutExpired:
        raise ValidationError(f"Import timed out after {timeout} seconds", field="timeout")
    
    except Exception as e:
        return {
            'database': db_config.get('repository', 'unknown'),
            'type': 'graphdb',
            'success': False,
            'error': str(e)
        }


def _run_neo4j_import(excel_path: Path, db_config: Dict[str, Any], batch_size: int, timeout: int) -> Dict[str, Any]:
    """Run Neo4j import with validation and error handling."""
    try:
        # Validate Neo4j configuration
        uri = db_config.get('uri', 'bolt://localhost:7687')
        validated_uri = DatabaseValidator.validate_neo4j_uri(uri)
        
        user = db_config.get('user', 'neo4j')
        password = db_config.get('password', '')
        
        # Build command
        cmd = [
            sys.executable, 'neo4j_importer.py',
            '--excel', str(excel_path),
            '--uri', validated_uri,
            '--user', user,
            '--batch-size', str(batch_size)
        ]
        
        if password:
            cmd.extend(['--password', password])
        
        logger.info(f"Running Neo4j import command: {' '.join(cmd[:4])}...")  # Don't log password
        
        # Set environment variables
        env = os.environ.copy()
        if password:
            env['NEO4J_PASSWORD'] = password
        
        # Execute command with timeout
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=PROJECT_ROOT,
            env=env
        )
        
        success = result.returncode == 0
        
        return {
            'database': 'neo4j',
            'type': 'neo4j',
            'success': success,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'return_code': result.returncode
        }
    
    except subprocess.TimeoutExpired:
        raise ValidationError(f"Import timed out after {timeout} seconds", field="timeout")
    
    except Exception as e:
        return {
            'database': 'neo4j',
            'type': 'neo4j',
            'success': False,
            'error': str(e)
        }


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint for monitoring."""
    try:
        # Check if data directory exists
        data_dir_exists = DATA_DIR.exists() and DATA_DIR.is_dir()
        
        # Check if scripts directory exists
        scripts_dir_exists = SCRIPTS_DIR.exists() and SCRIPTS_DIR.is_dir()
        
        health_status = {
            'status': 'healthy' if data_dir_exists and scripts_dir_exists else 'degraded',
            'timestamp': time.time(),
            'checks': {
                'data_directory': data_dir_exists,
                'scripts_directory': scripts_dir_exists
            }
        }
        
        status_code = 200 if health_status['status'] == 'healthy' else 503
        return jsonify(health_status), status_code
    
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': time.time()
        }), 503


if __name__ == '__main__':
    # Development server
    logger.info("Starting Windchill Importer Web Server")
    app.run(host='0.0.0.0', port=5050, debug=True)