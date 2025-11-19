"""
Enhanced spreadsheet loader with comprehensive validation and error handling.
Provides robust parsing of Excel files with detailed error reporting.
"""

import sys
import json
import warnings
from typing import Dict, List, Optional, Tuple, Set, Union
from pathlib import Path

import pandas as pd

# Import custom modules - use relative imports for package structure
try:
    from core.exceptions import (
        ExcelValidationError, DataProcessingError, ValidationError,
        FileValidationError
    )
    from core.validation import FileValidator, DataValidator
    from core.logging_config import get_logger, log_operation_start, log_operation_end, log_validation_error
except ImportError:
    # Fallback for when running as standalone script
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
    from core.exceptions import (
        ExcelValidationError, DataProcessingError, ValidationError,
        FileValidationError
    )
    from core.validation import FileValidator, DataValidator
    from core.logging_config import get_logger, log_operation_start, log_operation_end, log_validation_error

# Configure warnings
warnings.filterwarnings(
    "ignore",
    r"Workbook contains no default style.*",
    UserWarning,
    r"openpyxl\.styles\.stylesheet",
)

# Initialize logger
logger = get_logger(__name__)


def normalize_part_number(value) -> str:
    """
    Normalize part number with improved validation and error handling.
    
    Args:
        value: Raw part number value from Excel
        
    Returns:
        Normalized part number string
        
    Raises:
        ValidationError: If part number is invalid
    """
    if pd.isna(value):
        return ""
    
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value)
    
    result = str(value).strip()
    
    # Additional validation
    if result and len(result) > 50:
        logger.warning(f"Part number exceeds maximum length: {result[:50]}...")
    
    return result


class EnhancedSpreadsheetParser:
    """
    Enhanced spreadsheet parser with comprehensive validation and error handling.
    """
    
    def __init__(self, excel_path: str, warn_missing_required: bool = True):
        """
        Initialize the parser with validation.
        
        Args:
            excel_path: Path to Excel file
            warn_missing_required: Whether to warn about missing required columns
            
        Raises:
            ExcelValidationError: If Excel file is invalid
        """
        self.warn_missing_required = warn_missing_required
        
        # Validate Excel file
        try:
            self.excel_path = FileValidator.validate_excel_file(excel_path)
            logger.info(f"Validated Excel file: {self.excel_path}")
        except FileValidationError as e:
            logger.error(f"Excel file validation failed: {e}")
            raise
        
        # Cache for Excel file object
        self._excel_file = None
        self._sheet_names = None
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources."""
        if self._excel_file:
            try:
                self._excel_file.close()
            except Exception as e:
                logger.warning(f"Error closing Excel file: {e}")
    
    def get_sheet_names(self) -> List[str]:
        """
        Get sheet names with error handling and caching.
        
        Returns:
            List of sheet names
            
        Raises:
            ExcelValidationError: If Excel file cannot be read
        """
        if self._sheet_names is not None:
            return self._sheet_names
        
        try:
            if not self._excel_file:
                self._excel_file = pd.ExcelFile(self.excel_path)
            
            self._sheet_names = [str(s) for s in self._excel_file.sheet_names]
            logger.info(f"Found {len(self._sheet_names)} sheets in Excel file")
            return self._sheet_names
        
        except Exception as e:
            logger.error(f"Failed to read Excel file sheets: {e}")
            raise ExcelValidationError(f"Cannot read Excel file sheets: {str(e)}", value=str(self.excel_path))
    
    def validate_sheet_structure(self, sheet_name: str, df: pd.DataFrame) -> bool:
        """
        Validate that a sheet has the required structure.
        
        Args:
            sheet_name: Name of the sheet
            df: DataFrame to validate
            
        Returns:
            True if valid, False otherwise
        """
        required_columns = {"Number", "Name"}
        
        if df.empty or len(df.columns) == 0:
            logger.warning(f"Sheet '{sheet_name}' is empty or has no columns")
            return False
        
        if not required_columns.issubset(set(df.columns)):
            if self.warn_missing_required:
                logger.warning(
                    f"Sheet '{sheet_name}' missing required columns; found: {list(df.columns)}",
                    extra={'sheet': sheet_name, 'columns': list(df.columns)}
                )
            else:
                logger.debug(f"Skipping sheet '{sheet_name}'; required columns missing")
            return False
        
        return True
    
    def read_sheet_with_fallback(self, sheet_name: str) -> Optional[pd.DataFrame]:
        """
        Read a sheet with fallback strategies for different Excel formats.
        
        Args:
            sheet_name: Name of the sheet to read
            
        Returns:
            DataFrame if successful, None otherwise
        """
        try:
            # Try reading with skiprows first (common Windchill format)
            df = pd.read_excel(self.excel_path, sheet_name=sheet_name, skiprows=4)
            
            if df.empty or len(df.columns) == 0:
                logger.debug(f"Sheet '{sheet_name}' empty with skiprows=4, trying without skiprows")
                df = pd.read_excel(self.excel_path, sheet_name=sheet_name)
            
            # Handle header duplication issue
            if len(df.index) > 0:
                first = list(df.iloc[0].values)
                first_str = set(map(str, first))
                required = {"Number", "Name"}
                
                if required.issubset(first_str) and not required.issubset(set(map(str, df.columns))):
                    logger.debug(f"Sheet '{sheet_name}' appears to have duplicate headers, fixing")
                    df.columns = df.iloc[0]
                    df = df[1:].reset_index(drop=True)
            
            return df
        
        except Exception as e:
            logger.error(f"Failed to read sheet '{sheet_name}': {e}")
            return None
    
    def parse_parts(self, sheets: Optional[List[str]] = None) -> Dict[str, Dict[str, Optional[str]]]:
        """
        Parse parts from Excel sheets with comprehensive validation and error handling.
        
        Args:
            sheets: List of specific sheets to parse (None for all sheets)
            
        Returns:
            Dictionary of parts with their metadata
            
        Raises:
            DataProcessingError: If data processing fails
        """
        log_operation_start("parse_parts", sheets=sheets)
        
        parts: Dict[str, Dict[str, Optional[str]]] = {}
        sheet_names = sheets or self.get_sheet_names()
        
        logger.info(f"Parsing parts from {len(sheet_names)} sheets")
        
        for sheet_name in sheet_names:
            try:
                logger.debug(f"Processing sheet: {sheet_name}")
                
                df = self.read_sheet_with_fallback(sheet_name)
                if df is None:
                    logger.warning(f"Skipping sheet '{sheet_name}' - could not read")
                    continue
                
                if not self.validate_sheet_structure(sheet_name, df):
                    continue
                
                # Process rows in the sheet
                valid_rows = 0
                invalid_rows = 0
                
                for row_idx, row in df.iterrows():
                    try:
                        part_number = normalize_part_number(row.get("Number"))
                        if not part_number:
                            invalid_rows += 1
                            continue
                        
                        # Validate part number
                        try:
                            validated_part_number = DataValidator.validate_part_number(part_number)
                        except ValidationError as e:
                            logger.warning(
                                f"Invalid part number in sheet '{sheet_name}', row {row_idx + 1}: {e}",
                                extra={'sheet': sheet_name, 'row': row_idx + 1, 'part_number': part_number}
                            )
                            invalid_rows += 1
                            continue
                        
                        # Get part name
                        name = row.get("Name")
                        name = str(name).strip() if pd.notna(name) else validated_part_number
                        
                        # Validate part name
                        try:
                            validated_name = DataValidator.validate_part_name(name)
                        except ValidationError as e:
                            logger.warning(
                                f"Invalid part name in sheet '{sheet_name}', row {row_idx + 1}: {e}",
                                extra={'sheet': sheet_name, 'row': row_idx + 1, 'name': name}
                            )
                            validated_name = validated_part_number  # Fall back to part number
                        
                        # Determine part type from sheet name
                        part_type = self._determine_part_type(sheet_name)
                        
                        # Create part entry
                        parts[validated_part_number] = {
                            "name": validated_name,
                            "type": self._safe_get_string(row, "Type"),
                            "source": self._safe_get_string(row, "Source", "windchill"),
                            "view": self._safe_get_string(row, "View"),
                            "state": self._safe_get_string(row, "State"),
                            "revision": self._safe_get_string(row, "Revision"),
                            "container": self._safe_get_string(row, "Container"),
                            "part_type": part_type,
                            "sheet": sheet_name,
                            "row": row_idx + 1,
                        }
                        
                        valid_rows += 1
                    
                    except Exception as e:
                        logger.error(
                            f"Error processing row {row_idx + 1} in sheet '{sheet_name}': {e}",
                            extra={'sheet': sheet_name, 'row': row_idx + 1, 'error': str(e)}
                        )
                        invalid_rows += 1
                
                logger.info(
                    f"Sheet '{sheet_name}' processing complete: {valid_rows} valid, {invalid_rows} invalid rows",
                    extra={'sheet': sheet_name, 'valid_rows': valid_rows, 'invalid_rows': invalid_rows}
                )
            
            except Exception as e:
                logger.error(f"Error processing sheet '{sheet_name}': {e}")
                continue
        
        log_operation_end("parse_parts", success=True, parts_count=len(parts))
        logger.info(f"Total parts parsed: {len(parts)}")
        return parts
    
    def _determine_part_type(self, sheet_name: str) -> Optional[str]:
        """Determine part type based on sheet name."""
        sheet_lower = sheet_name.lower()
        
        if "mechanicalpart" in sheet_lower:
            return "MechanicalPart"
        elif "softwarepart" in sheet_lower:
            return "SoftwarePart"
        elif "variant" in sheet_lower:
            return "Variant"
        elif "wtpart" in sheet_lower:
            return "WTPart"
        elif "basicnode" in sheet_lower:
            return "BasicNode"
        elif "structurenode" in sheet_lower:
            return "StructureNode"
        
        return None
    
    def _safe_get_string(self, row: pd.Series, column: str, default: Optional[str] = None) -> Optional[str]:
        """Safely get a string value from a DataFrame row."""
        try:
            value = row.get(column)
            if pd.isna(value):
                return default
            
            result = str(value).strip()
            return result if result else default
        
        except Exception:
            return default
    
    def build_cross_index(self, sheets: Optional[List[str]] = None) -> Tuple[Dict[str, str], Dict[str, List[Dict[str, Optional[str]]]]]:
        """
        Build cross-reference index between part numbers and names.
        
        Args:
            sheets: List of specific sheets to process (None for all sheets)
            
        Returns:
            Tuple of (part_number_to_name, name_to_metadata) dictionaries
        """
        log_operation_start("build_cross_index", sheets=sheets)
        
        pn_to_name: Dict[str, str] = {}
        name_sources: Dict[str, List[Dict[str, Optional[str]]]] = {}
        
        sheet_names = sheets or self.get_sheet_names()
        
        for sheet_name in sheet_names:
            try:
                df = self.read_sheet_with_fallback(sheet_name)
                if df is None:
                    continue
                
                if not self.validate_sheet_structure(sheet_name, df):
                    continue
                
                cols = set(df.columns)
                if {"Number", "Name"}.issubset(cols):
                    for row_idx, row in df.iterrows():
                        try:
                            pn = normalize_part_number(row.get("Number"))
                            nm = row.get("Name")
                            
                            if pn and pd.notna(nm):
                                name = str(nm).strip()
                                pn_to_name[pn] = name
                                
                                meta = {
                                    "sheet": sheet_name,
                                    "revision": self._safe_get_string(row, "Revision"),
                                    "view": self._safe_get_string(row, "View"),
                                    "container": self._safe_get_string(row, "Container"),
                                    "row": row_idx + 1,
                                }
                                
                                if name not in name_sources:
                                    name_sources[name] = [meta]
                                else:
                                    name_sources[name].append(meta)
                        
                        except Exception as e:
                            logger.warning(
                                f"Error processing row {row_idx + 1} in cross-index for sheet '{sheet_name}': {e}"
                            )
            
            except Exception as e:
                logger.error(f"Error building cross-index for sheet '{sheet_name}': {e}")
                continue
        
        log_operation_end("build_cross_index", success=True, 
                         parts_count=len(pn_to_name), names_count=len(name_sources))
        logger.info(f"Cross-index built: {len(pn_to_name)} parts, {len(name_sources)} unique names")
        
        return pn_to_name, name_sources