"""
Test suite for the enhanced Windchill XLSX to Knowledge Graph importer.
Demonstrates improved error handling, validation, and logging.
"""

import unittest
import tempfile
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from exceptions import (
    ValidationError, FileValidationError, ExcelValidationError, 
    CSVValidationError, DatabaseConnectionError, ConfigurationError
)
from validation import FileValidator, DatabaseValidator, ConfigurationValidator, DataValidator
from logging_config import setup_logging, get_logger, log_operation_start, log_operation_end
from enhanced_spreadsheet_loader import EnhancedSpreadsheetParser, normalize_part_number


class TestExceptions(unittest.TestCase):
    """Test custom exception hierarchy."""
    
    def test_validation_error(self):
        """Test ValidationError with field and value."""
        error = ValidationError("Invalid input", field="part_number", value="123")
        self.assertEqual(str(error), "Invalid input")
        self.assertEqual(error.field, "part_number")
        self.assertEqual(error.value, "123")
    
    def test_file_validation_error(self):
        """Test FileValidationError inheritance."""
        error = FileValidationError("File not found", field="path", value="/nonexistent")
        self.assertIsInstance(error, ValidationError)
        self.assertEqual(error.field, "path")
    
    def test_excel_validation_error(self):
        """Test ExcelValidationError inheritance."""
        error = ExcelValidationError("Invalid Excel format", field="extension", value=".txt")
        self.assertIsInstance(error, FileValidationError)
        self.assertIsInstance(error, ValidationError)
        self.assertEqual(error.field, "extension")
    
    def test_database_connection_error(self):
        """Test DatabaseConnectionError with database type."""
        error = DatabaseConnectionError("Connection failed", database_type="neo4j", url="bolt://localhost")
        self.assertEqual(error.database_type, "neo4j")
        self.assertEqual(error.url, "bolt://localhost")


class TestFileValidation(unittest.TestCase):
    """Test file validation functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_validate_file_exists_success(self):
        """Test successful file existence validation."""
        test_file = self.temp_path / "test.txt"
        test_file.write_text("test content")
        
        result = FileValidator.validate_file_exists(test_file)
        self.assertEqual(result, test_file)
    
    def test_validate_file_exists_not_found(self):
        """Test file not found validation."""
        test_file = self.temp_path / "nonexistent.txt"
        
        with self.assertRaises(FileValidationError) as cm:
            FileValidator.validate_file_exists(test_file)
        
        self.assertIn("does not exist", str(cm.exception))
    
    def test_validate_excel_file_valid(self):
        """Test valid Excel file validation."""
        # Create a valid Excel file
        test_excel = self.temp_path / "test.xlsx"
        df = pd.DataFrame({
            'Number': ['123', '456'],
            'Name': ['Part 1', 'Part 2']
        })
        df.to_excel(test_excel, index=False)
        
        result = FileValidator.validate_excel_file(test_excel)
        self.assertEqual(result, test_excel)
    
    def test_validate_excel_file_invalid_extension(self):
        """Test Excel file with invalid extension."""
        test_file = self.temp_path / "test.txt"
        test_file.write_text("not an excel file")
        
        with self.assertRaises(ExcelValidationError) as cm:
            FileValidator.validate_excel_file(test_file)
        
        self.assertIn("Invalid Excel file extension", str(cm.exception))
    
    def test_validate_csv_file_valid(self):
        """Test valid CSV file validation."""
        test_csv = self.temp_path / "test.csv"
        df = pd.DataFrame({
            'Parent': ['123', '456'],
            'Child': ['789', '012']
        })
        df.to_csv(test_csv, index=False)
        
        result = FileValidator.validate_csv_file(test_csv, required_columns=['Parent', 'Child'])
        self.assertEqual(result, test_csv)
    
    def test_validate_csv_file_missing_columns(self):
        """Test CSV file with missing required columns."""
        test_csv = self.temp_path / "test.csv"
        df = pd.DataFrame({
            'Parent': ['123', '456'],
            'Wrong': ['789', '012']
        })
        df.to_csv(test_csv, index=False)
        
        with self.assertRaises(CSVValidationError) as cm:
            FileValidator.validate_csv_file(test_csv, required_columns=['Parent', 'Child'])
        
        self.assertIn("Missing required columns", str(cm.exception))


class TestDatabaseValidation(unittest.TestCase):
    """Test database validation functionality."""
    
    def test_validate_graphdb_url_valid(self):
        """Test valid GraphDB URL validation."""
        url = "http://localhost:7200"
        result = DatabaseValidator.validate_graphdb_url(url)
        self.assertEqual(result, url)
    
    def test_validate_graphdb_url_invalid_protocol(self):
        """Test GraphDB URL with invalid protocol."""
        url = "ftp://localhost:7200"
        
        with self.assertRaises(ConfigurationError) as cm:
            DatabaseValidator.validate_graphdb_url(url)
        
        self.assertIn("must use HTTP/HTTPS protocol", str(cm.exception))
    
    def test_validate_neo4j_uri_valid(self):
        """Test valid Neo4j URI validation."""
        uri = "bolt://localhost:7687"
        result = DatabaseValidator.validate_neo4j_uri(uri)
        self.assertEqual(result, uri)
    
    def test_validate_neo4j_uri_invalid_protocol(self):
        """Test Neo4j URI with invalid protocol."""
        uri = "http://localhost:7687"
        
        with self.assertRaises(ConfigurationError) as cm:
            DatabaseValidator.validate_neo4j_uri(uri)
        
        self.assertIn("must use BOLT protocol", str(cm.exception))
    
    def test_validate_repository_name_valid(self):
        """Test valid repository name validation."""
        name = "test-repo_123"
        result = DatabaseValidator.validate_repository_name(name)
        self.assertEqual(result, name)
    
    def test_validate_repository_name_empty(self):
        """Test empty repository name validation."""
        with self.assertRaises(ConfigurationError) as cm:
            DatabaseValidator.validate_repository_name("")
        
        self.assertIn("cannot be empty", str(cm.exception))
    
    def test_validate_repository_name_invalid_chars(self):
        """Test repository name with invalid characters."""
        with self.assertRaises(ConfigurationError) as cm:
            DatabaseValidator.validate_repository_name("test@repo")
        
        self.assertIn("can only contain letters, numbers, hyphens, and underscores", str(cm.exception))


class TestConfigurationValidation(unittest.TestCase):
    """Test configuration validation functionality."""
    
    def test_validate_batch_size_valid(self):
        """Test valid batch size validation."""
        result = ConfigurationValidator.validate_batch_size(1000)
        self.assertEqual(result, 1000)
    
    def test_validate_batch_size_string_conversion(self):
        """Test batch size string to int conversion."""
        result = ConfigurationValidator.validate_batch_size("1000")
        self.assertEqual(result, 1000)
    
    def test_validate_batch_size_invalid_string(self):
        """Test invalid batch size string."""
        with self.assertRaises(ConfigurationError) as cm:
            ConfigurationValidator.validate_batch_size("invalid")
        
        self.assertIn("must be an integer", str(cm.exception))
    
    def test_validate_batch_size_negative(self):
        """Test negative batch size validation."""
        with self.assertRaises(ConfigurationError) as cm:
            ConfigurationValidator.validate_batch_size(-100)
        
        self.assertIn("must be positive", str(cm.exception))
    
    def test_validate_batch_size_too_large(self):
        """Test batch size too large validation."""
        with self.assertRaises(ConfigurationError) as cm:
            ConfigurationValidator.validate_batch_size(20000)
        
        self.assertIn("too large", str(cm.exception))


class TestDataValidation(unittest.TestCase):
    """Test data validation functionality."""
    
    def test_validate_part_number_valid(self):
        """Test valid part number validation."""
        result = DataValidator.validate_part_number("123-ABC")
        self.assertEqual(result, "123-ABC")
    
    def test_validate_part_number_empty(self):
        """Test empty part number validation."""
        with self.assertRaises(ValidationError) as cm:
            DataValidator.validate_part_number("")
        
        self.assertIn("cannot be empty", str(cm.exception))
    
    def test_validate_part_number_too_long(self):
        """Test part number too long validation."""
        long_part = "A" * 60
        result = DataValidator.validate_part_number(long_part)
        self.assertEqual(len(result), 50)  # Should be truncated
    
    def test_validate_part_name_valid(self):
        """Test valid part name validation."""
        result = DataValidator.validate_part_name("Engine Assembly")
        self.assertEqual(result, "Engine Assembly")
    
    def test_validate_bom_relationship_valid(self):
        """Test valid BOM relationship validation."""
        parent, child = DataValidator.validate_bom_relationship("123", "456")
        self.assertEqual(parent, "123")
        self.assertEqual(child, "456")
    
    def test_validate_bom_relationship_same_parts(self):
        """Test BOM relationship with same parent and child."""
        with self.assertRaises(ValidationError) as cm:
            DataValidator.validate_bom_relationship("123", "123")
        
        self.assertIn("cannot be the same", str(cm.exception))


class TestEnhancedSpreadsheetLoader(unittest.TestCase):
    """Test enhanced spreadsheet loader functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)
        
        # Setup logging for tests
        setup_logging(level='DEBUG', include_console=False)
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_normalize_part_number_float(self):
        """Test part number normalization with float values."""
        result = normalize_part_number(123.0)
        self.assertEqual(result, "123")
        
        result = normalize_part_number(123.5)
        self.assertEqual(result, "123.5")
    
    def test_normalize_part_number_string(self):
        """Test part number normalization with string values."""
        result = normalize_part_number(" 123 ")
        self.assertEqual(result, "123")
    
    def test_enhanced_parser_initialization(self):
        """Test enhanced parser initialization with validation."""
        # Create valid Excel file
        test_excel = self.temp_path / "test.xlsx"
        df = pd.DataFrame({
            'Number': ['123', '456'],
            'Name': ['Part 1', 'Part 2']
        })
        df.to_excel(test_excel, index=False)
        
        parser = EnhancedSpreadsheetParser(str(test_excel))
        self.assertEqual(parser.excel_path, test_excel)
    
    def test_enhanced_parser_invalid_excel(self):
        """Test enhanced parser with invalid Excel file."""
        test_file = self.temp_path / "test.txt"
        test_file.write_text("not an excel file")
        
        with self.assertRaises(ExcelValidationError):
            EnhancedSpreadsheetParser(str(test_file))
    
    def test_validate_sheet_structure_valid(self):
        """Test sheet structure validation with valid data."""
        test_excel = self.temp_path / "test.xlsx"
        df = pd.DataFrame({
            'Number': ['123', '456'],
            'Name': ['Part 1', 'Part 2']
        })
        df.to_excel(test_excel, index=False)
        
        parser = EnhancedSpreadsheetParser(str(test_excel))
        
        # Create test DataFrame
        test_df = pd.DataFrame({
            'Number': ['123'],
            'Name': ['Test Part']
        })
        
        result = parser.validate_sheet_structure("TestSheet", test_df)
        self.assertTrue(result)
    
    def test_validate_sheet_structure_missing_columns(self):
        """Test sheet structure validation with missing columns."""
        test_excel = self.temp_path / "test.xlsx"
        df = pd.DataFrame({
            'Number': ['123', '456'],
            'Name': ['Part 1', 'Part 2']
        })
        df.to_excel(test_excel, index=False)
        
        parser = EnhancedSpreadsheetParser(str(test_excel))
        
        # Create test DataFrame with missing columns
        test_df = pd.DataFrame({
            'Wrong': ['123']
        })
        
        result = parser.validate_sheet_structure("TestSheet", test_df)
        self.assertFalse(result)
    
    def test_parse_parts_with_valid_data(self):
        """Test parsing parts with valid Excel data."""
        test_excel = self.temp_path / "test.xlsx"
        df = pd.DataFrame({
            'Number': ['123', '456', '789'],
            'Name': ['Engine', 'Transmission', 'Wheel'],
            'Type': ['Mechanical', 'Mechanical', 'Mechanical'],
            'Source': ['Windchill', 'Windchill', 'Windchill']
        })
        df.to_excel(test_excel, index=False, sheet_name='MechanicalPart')
        
        parser = EnhancedSpreadsheetParser(str(test_excel))
        parts = parser.parse_parts()
        
        self.assertEqual(len(parts), 3)
        self.assertIn('123', parts)
        self.assertEqual(parts['123']['name'], 'Engine')
        self.assertEqual(parts['123']['type'], 'Mechanical')
        self.assertEqual(parts['123']['part_type'], 'MechanicalPart')
    
    def test_parse_parts_with_invalid_data(self):
        """Test parsing parts with invalid data."""
        test_excel = self.temp_path / "test.xlsx"
        df = pd.DataFrame({
            'Number': ['123', '', '456'],  # Empty part number
            'Name': ['Engine', 'Transmission', ''],  # Empty name
            'Type': ['Mechanical', 'Mechanical', 'Mechanical']
        })
        df.to_excel(test_excel, index=False)
        
        parser = EnhancedSpreadsheetParser(str(test_excel))
        parts = parser.parse_parts()
        
        # Should only have valid parts
        self.assertEqual(len(parts), 2)  # 123 and 456, but not the empty one
        self.assertIn('123', parts)
        self.assertIn('456', parts)
    
    def test_build_cross_index(self):
        """Test building cross-reference index."""
        test_excel = self.temp_path / "test.xlsx"
        df = pd.DataFrame({
            'Number': ['123', '456'],
            'Name': ['Engine', 'Transmission'],
            'Revision': ['A', 'B']
        })
        df.to_excel(test_excel, index=False)
        
        parser = EnhancedSpreadsheetParser(str(test_excel))
        pn_to_name, name_sources = parser.build_cross_index()
        
        self.assertEqual(len(pn_to_name), 2)
        self.assertEqual(pn_to_name['123'], 'Engine')
        self.assertEqual(pn_to_name['456'], 'Transmission')
        
        self.assertEqual(len(name_sources), 2)
        self.assertIn('Engine', name_sources)
        self.assertIn('Transmission', name_sources)


class TestLogging(unittest.TestCase):
    """Test logging configuration and functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_setup_logging_basic(self):
        """Test basic logging setup."""
        setup_logging(level='INFO', include_console=False)
        logger = get_logger(__name__)
        
        # The logger level should be 0 (NOTSET) because we set the root logger level
        # Individual loggers inherit from the root logger
        self.assertEqual(logger.level, 0)  # NOTSET - inherits from root
    
    def test_setup_logging_with_file(self):
        """Test logging setup with file output."""
        log_file = Path(self.temp_dir) / "test.log"
        setup_logging(level='DEBUG', log_file=str(log_file), include_console=False)
        
        logger = get_logger(__name__)
        logger.info("Test message")
        
        # Check that log file was created and contains the message
        self.assertTrue(log_file.exists())
        log_content = log_file.read_text()
        self.assertIn("Test message", log_content)
    
    def test_log_operation_context(self):
        """Test operation logging with context."""
        setup_logging(level='INFO', include_console=False)
        
        # Test successful operation
        log_operation_start("test_operation", test_param="value")
        log_operation_end("test_operation", success=True, duration=1.5, result="success")
        
        # Test failed operation
        log_operation_start("failed_operation")
        log_operation_end("failed_operation", success=False, error="Test error")


if __name__ == '__main__':
    # Run all tests
    unittest.main(verbosity=2)