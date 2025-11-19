"""
Validation utilities for the Windchill XLSX to Knowledge Graph importer.
Provides comprehensive validation for files, data, and configurations.
"""

import os
import re
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from urllib.parse import urlparse

from .exceptions import (
    ValidationError, FileValidationError, ExcelValidationError, CSVValidationError,
    ConfigurationError, DatabaseConnectionError
)
from .logging_config import get_logger


class FileValidator:
    """Validates input files for the importer."""
    
    # Maximum file sizes (in bytes)
    MAX_EXCEL_SIZE = 100 * 1024 * 1024  # 100MB
    MAX_CSV_SIZE = 50 * 1024 * 1024     # 50MB
    
    # Allowed file extensions
    ALLOWED_EXCEL_EXTENSIONS = {'.xlsx', '.xlsm', '.xls'}
    ALLOWED_CSV_EXTENSIONS = {'.csv', '.tsv'}
    
    @staticmethod
    def validate_file_exists(file_path: Union[str, Path]) -> Path:
        """Validate that a file exists and is readable."""
        path = Path(file_path)
        
        if not path.exists():
            raise FileValidationError(f"File does not exist: {file_path}", value=str(file_path))
        
        if not path.is_file():
            raise FileValidationError(f"Path is not a file: {file_path}", value=str(file_path))
        
        if not os.access(path, os.R_OK):
            raise FileValidationError(f"File is not readable: {file_path}", value=str(file_path))
        
        return path
    
    @staticmethod
    def validate_excel_file(file_path: Union[str, Path]) -> Path:
        """Comprehensive validation for Excel files."""
        path = FileValidator.validate_file_exists(file_path)
        
        # Check file extension
        if path.suffix.lower() not in FileValidator.ALLOWED_EXCEL_EXTENSIONS:
            raise ExcelValidationError(
                f"Invalid Excel file extension: {path.suffix}. Allowed: {FileValidator.ALLOWED_EXCEL_EXTENSIONS}",
                field="file_extension",
                value=path.suffix
            )
        
        # Check file size
        file_size = path.stat().st_size
        if file_size > FileValidator.MAX_EXCEL_SIZE:
            raise ExcelValidationError(
                f"Excel file too large: {file_size / 1024 / 1024:.1f}MB (max: {FileValidator.MAX_EXCEL_SIZE / 1024 / 1024}MB)",
                field="file_size",
                value=str(file_size)
            )
        
        # Try to open and validate Excel structure
        try:
            with pd.ExcelFile(path) as xls:
                sheet_names = xls.sheet_names
                if not sheet_names:
                    raise ExcelValidationError("Excel file contains no worksheets", value=str(path))
                
                # Validate that at least one sheet has required columns
                has_valid_sheet = False
                for sheet_name in sheet_names:
                    try:
                        df = pd.read_excel(xls, sheet_name=sheet_name, nrows=1)
                        if {"Number", "Name"}.issubset(set(df.columns)):
                            has_valid_sheet = True
                            break
                    except Exception:
                        continue
                
                if not has_valid_sheet:
                    raise ExcelValidationError(
                        "No worksheet contains required columns 'Number' and 'Name'",
                        value=str(path)
                    )
        
        except Exception as e:
            if isinstance(e, ExcelValidationError):
                raise
            raise ExcelValidationError(f"Invalid Excel file format: {str(e)}", value=str(path))
        
        return path
    
    @staticmethod
    def validate_csv_file(file_path: Union[str, Path], required_columns: Optional[List[str]] = None) -> Path:
        """Comprehensive validation for CSV files."""
        path = FileValidator.validate_file_exists(file_path)
        
        # Check file extension
        if path.suffix.lower() not in FileValidator.ALLOWED_CSV_EXTENSIONS:
            raise CSVValidationError(
                f"Invalid CSV file extension: {path.suffix}. Allowed: {FileValidator.ALLOWED_CSV_EXTENSIONS}",
                field="file_extension",
                value=path.suffix
            )
        
        # Check file size
        file_size = path.stat().st_size
        if file_size > FileValidator.MAX_CSV_SIZE:
            raise CSVValidationError(
                f"CSV file too large: {file_size / 1024 / 1024:.1f}MB (max: {FileValidator.MAX_CSV_SIZE / 1024 / 1024}MB)",
                field="file_size",
                value=str(file_size)
            )
        
        # Try to parse and validate CSV structure
        try:
            df = pd.read_csv(path, nrows=1)
            
            if df.empty:
                raise CSVValidationError("CSV file is empty", value=str(path))
            
            if required_columns:
                missing_columns = set(required_columns) - set(df.columns)
                if missing_columns:
                    raise CSVValidationError(
                        f"Missing required columns: {missing_columns}",
                        field="columns",
                        value=str(missing_columns)
                    )
        
        except pd.errors.ParserError as e:
            raise CSVValidationError(f"Invalid CSV format: {str(e)}", value=str(path))
        except Exception as e:
            if isinstance(e, CSVValidationError):
                raise
            raise CSVValidationError(f"Error reading CSV file: {str(e)}", value=str(path))
        
        return path


class DatabaseValidator:
    """Validates database connections and configurations."""
    
    @staticmethod
    def validate_graphdb_url(url: str) -> str:
        """Validate GraphDB URL format and accessibility."""
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                raise ConfigurationError(f"Invalid GraphDB URL format: {url}", value=url)
            
            if parsed.scheme not in ['http', 'https']:
                raise ConfigurationError(
                    f"GraphDB URL must use HTTP/HTTPS protocol: {url}",
                    field="protocol",
                    value=parsed.scheme
                )
            
            return url
        
        except Exception as e:
            if isinstance(e, ConfigurationError):
                raise
            raise ConfigurationError(f"Invalid GraphDB URL: {str(e)}", value=url)
    
    @staticmethod
    def validate_neo4j_uri(uri: str) -> str:
        """Validate Neo4j URI format."""
        try:
            parsed = urlparse(uri)
            if not parsed.scheme or not parsed.netloc:
                raise ConfigurationError(f"Invalid Neo4j URI format: {uri}", value=uri)
            
            if parsed.scheme not in ['bolt', 'bolt+s', 'neo4j', 'neo4j+s']:
                raise ConfigurationError(
                    f"Neo4j URI must use BOLT protocol: {uri}",
                    field="protocol",
                    value=parsed.scheme
                )
            
            return uri
        
        except Exception as e:
            if isinstance(e, ConfigurationError):
                raise
            raise ConfigurationError(f"Invalid Neo4j URI: {str(e)}", value=uri)
    
    @staticmethod
    def validate_repository_name(repo_name: str) -> str:
        """Validate repository/database name."""
        if not repo_name or not repo_name.strip():
            raise ConfigurationError("Repository name cannot be empty", value=repo_name)
        
        # Basic validation for repository names
        if not re.match(r'^[a-zA-Z0-9_-]+$', repo_name):
            raise ConfigurationError(
                "Repository name can only contain letters, numbers, hyphens, and underscores",
                field="repository_name",
                value=repo_name
            )
        
        if len(repo_name) > 64:
            raise ConfigurationError(
                "Repository name too long (max 64 characters)",
                field="repository_name",
                value=repo_name
            )
        
        return repo_name.strip()


class DataValidator:
    """Validates data content and structure."""
    
    @staticmethod
    def validate_part_number(part_number: str) -> str:
        """Validate part number format."""
        if not part_number or not part_number.strip():
            raise ValidationError("Part number cannot be empty", value=part_number)
        
        part_number = part_number.strip()
        
        # Basic validation - can be customized based on requirements
        if len(part_number) > 50:
            # Truncate instead of raising error
            logger = get_logger(__name__)
            logger.warning(
                f"Part number too long, truncating to 50 characters: {part_number[:50]}...",
                extra={'original_length': len(part_number), 'truncated_length': 50}
            )
            part_number = part_number[:50]
        
        # Check for invalid characters
        if re.search(r'[<>:"|?*\x00-\x1f]', part_number):
            raise ValidationError(
                "Part number contains invalid characters",
                field="part_number",
                value=part_number
            )
        
        return part_number
    
    @staticmethod
    def validate_part_name(part_name: str) -> str:
        """Validate part name format."""
        if not part_name or not part_name.strip():
            raise ValidationError("Part name cannot be empty", value=part_name)
        
        part_name = part_name.strip()
        
        if len(part_name) > 200:
            raise ValidationError(
                "Part name too long (max 200 characters)",
                field="part_name",
                value=part_name
            )
        
        return part_name
    
    @staticmethod
    def validate_bom_relationship(parent: str, child: str) -> tuple[str, str]:
        """Validate BOM parent-child relationship."""
        validated_parent = DataValidator.validate_part_number(parent)
        validated_child = DataValidator.validate_part_number(child)
        
        if validated_parent == validated_child:
            raise ValidationError(
                "Parent and child part numbers cannot be the same",
                field="bom_relationship",
                value=f"{parent} -> {child}"
            )
        
        return validated_parent, validated_child


class ConfigurationValidator:
    """Validates application configuration."""
    
    @staticmethod
    def validate_batch_size(batch_size: int) -> int:
        """Validate batch size configuration."""
        if not isinstance(batch_size, int):
            try:
                batch_size = int(batch_size)
            except (ValueError, TypeError):
                raise ConfigurationError(
                    "Batch size must be an integer",
                    field="batch_size",
                    value=str(batch_size)
                )
        
        if batch_size <= 0:
            raise ConfigurationError(
                "Batch size must be positive",
                field="batch_size",
                value=str(batch_size)
            )
        
        if batch_size > 10000:
            raise ConfigurationError(
                "Batch size too large (max 10000)",
                field="batch_size",
                value=str(batch_size)
            )
        
        return batch_size
    
    @staticmethod
    def validate_timeout(timeout: int) -> int:
        """Validate timeout configuration."""
        if not isinstance(timeout, int):
            try:
                timeout = int(timeout)
            except (ValueError, TypeError):
                raise ConfigurationError(
                    "Timeout must be an integer",
                    field="timeout",
                    value=str(timeout)
                )
        
        if timeout <= 0:
            raise ConfigurationError(
                "Timeout must be positive",
                field="timeout",
                value=str(timeout)
            )
        
        if timeout > 300:  # 5 minutes max
            raise ConfigurationError(
                "Timeout too large (max 300 seconds)",
                field="timeout",
                value=str(timeout)
            )
        
        return timeout