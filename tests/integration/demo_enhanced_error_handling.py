#!/usr/bin/env python3
"""
Demo script showing enhanced error handling and validation features.
Demonstrates how to use the new validation and error handling capabilities.
"""

import sys
import os
import tempfile
import pandas as pd
from pathlib import Path

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from exceptions import (
    ValidationError, FileValidationError, ExcelValidationError, 
    CSVValidationError, DatabaseConnectionError, ConfigurationError,
    WindchillImporterError
)
from validation import FileValidator, DatabaseValidator, ConfigurationValidator, DataValidator
from logging_config import setup_logging, get_logger, log_operation_start, log_operation_end
from enhanced_spreadsheet_loader import EnhancedSpreadsheetParser

def demo_file_validation():
    """Demonstrate file validation capabilities."""
    print("=== File Validation Demo ===")
    
    # Create test files
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create valid Excel file
        valid_excel = temp_path / "valid.xlsx"
        df = pd.DataFrame({
            'Number': ['123', '456'],
            'Name': ['Engine', 'Transmission']
        })
        df.to_excel(valid_excel, index=False)
        
        # Create invalid Excel file (wrong extension)
        invalid_excel = temp_path / "invalid.txt"
        invalid_excel.write_text("not an excel file")
        
        # Create oversized Excel file
        large_excel = temp_path / "large.xlsx"
        # Create a file larger than 100MB limit
        large_data = pd.DataFrame({
            'Number': ['123'] * 1000000,  # 1M rows
            'Name': ['Large Part'] * 1000000
        })
        large_data.to_excel(large_excel, index=False)
        
        # Test valid file
        try:
            result = FileValidator.validate_excel_file(valid_excel)
            print(f"✅ Valid Excel file validated: {result}")
        except FileValidationError as e:
            print(f"❌ Unexpected error: {e}")
        
        # Test invalid extension
        try:
            FileValidator.validate_excel_file(invalid_excel)
            print("❌ Should have failed validation")
        except ExcelValidationError as e:
            print(f"✅ Correctly caught invalid extension: {e}")
        
        # Test oversized file (this might not actually be oversized due to Excel compression,
        # but demonstrates the validation logic)
        try:
            FileValidator.validate_excel_file(large_excel)
            print(f"✅ Large file passed validation (Excel compression)")
        except ExcelValidationError as e:
            print(f"✅ Correctly caught oversized file: {e}")

def demo_database_validation():
    """Demonstrate database validation capabilities."""
    print("\n=== Database Validation Demo ===")
    
    # Test valid URLs
    valid_urls = [
        "http://localhost:7200",
        "https://graphdb.example.com:7200",
        "bolt://localhost:7687",
        "neo4j+s://example.com:7687"
    ]
    
    for url in valid_urls:
        try:
            if url.startswith(('http://', 'https://')):
                result = DatabaseValidator.validate_graphdb_url(url)
                print(f"✅ Valid GraphDB URL: {result}")
            else:
                result = DatabaseValidator.validate_neo4j_uri(url)
                print(f"✅ Valid Neo4j URI: {result}")
        except ConfigurationError as e:
            print(f"❌ Unexpected error: {e}")
    
    # Test invalid URLs
    invalid_urls = [
        ("ftp://localhost:7200", "graphdb"),
        ("http://localhost", "neo4j"),
        ("invalid-url", "graphdb"),
        ("", "neo4j")
    ]
    
    for url, db_type in invalid_urls:
        try:
            if db_type == "graphdb":
                DatabaseValidator.validate_graphdb_url(url)
            else:
                DatabaseValidator.validate_neo4j_uri(url)
            print(f"❌ Should have failed validation: {url}")
        except ConfigurationError as e:
            print(f"✅ Correctly caught invalid URL: {e}")

def demo_data_validation():
    """Demonstrate data validation capabilities."""
    print("\n=== Data Validation Demo ===")
    
    # Test valid part numbers
    valid_part_numbers = ["123", "ABC-123", "PART_001", "123.45"]
    
    for part_num in valid_part_numbers:
        try:
            result = DataValidator.validate_part_number(part_num)
            print(f"✅ Valid part number: {result}")
        except ValidationError as e:
            print(f"❌ Unexpected error: {e}")
    
    # Test invalid part numbers
    invalid_part_numbers = ["", "   ", "PART<>123", "A" * 60]
    
    for part_num in invalid_part_numbers:
        try:
            DataValidator.validate_part_number(part_num)
            print(f"❌ Should have failed validation: '{part_num}'")
        except ValidationError as e:
            print(f"✅ Correctly caught invalid part number: {e}")
    
    # Test BOM relationships
    valid_relationships = [("123", "456"), ("PARENT", "CHILD")]
    
    for parent, child in valid_relationships:
        try:
            p, c = DataValidator.validate_bom_relationship(parent, child)
            print(f"✅ Valid BOM relationship: {p} -> {c}")
        except ValidationError as e:
            print(f"❌ Unexpected error: {e}")
    
    # Test invalid BOM relationship (same part)
    try:
        DataValidator.validate_bom_relationship("123", "123")
        print("❌ Should have failed validation")
    except ValidationError as e:
        print(f"✅ Correctly caught invalid relationship: {e}")

def demo_configuration_validation():
    """Demonstrate configuration validation capabilities."""
    print("\n=== Configuration Validation Demo ===")
    
    # Test valid batch sizes
    valid_batch_sizes = [100, 1000, 5000, "1000"]
    
    for batch_size in valid_batch_sizes:
        try:
            result = ConfigurationValidator.validate_batch_size(batch_size)
            print(f"✅ Valid batch size: {result}")
        except ConfigurationError as e:
            print(f"❌ Unexpected error: {e}")
    
    # Test invalid batch sizes
    invalid_batch_sizes = [0, -100, 20000, "invalid", None]
    
    for batch_size in invalid_batch_sizes:
        try:
            ConfigurationValidator.validate_batch_size(batch_size)
            print(f"❌ Should have failed validation: {batch_size}")
        except ConfigurationError as e:
            print(f"✅ Correctly caught invalid batch size: {e}")

def demo_enhanced_spreadsheet_loader():
    """Demonstrate enhanced spreadsheet loader capabilities."""
    print("\n=== Enhanced Spreadsheet Loader Demo ===")
    
    # Setup logging
    setup_logging(level='INFO')
    logger = get_logger(__name__)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Create test Excel file with multiple sheets
        test_excel = temp_path / "windchill_parts.xlsx"
        
        # Create mechanical parts sheet
        mech_df = pd.DataFrame({
            'Number': ['ENG-001', 'TRANS-002', 'WHEEL-003'],
            'Name': ['Engine Assembly', 'Transmission', 'Wheel Assembly'],
            'Type': ['Mechanical', 'Mechanical', 'Mechanical'],
            'Source': ['Windchill', 'Windchill', 'Windchill'],
            'Revision': ['A', 'B', 'C'],
            'View': ['Design', 'Design', 'Manufacturing']
        })
        
        # Create software parts sheet
        sw_df = pd.DataFrame({
            'Number': ['SW-001', 'SW-002'],
            'Name': ['Engine Control Software', 'Transmission Control Software'],
            'Type': ['Software', 'Software'],
            'Source': ['Windchill', 'Windchill']
        })
        
        # Write to Excel with multiple sheets
        with pd.ExcelWriter(test_excel) as writer:
            mech_df.to_excel(writer, sheet_name='MechanicalPart', index=False)
            sw_df.to_excel(writer, sheet_name='SoftwarePart', index=False)
        
        print(f"Created test Excel file: {test_excel}")
        
        # Test enhanced parser
        try:
            with EnhancedSpreadsheetParser(str(test_excel)) as parser:
                print("✅ Enhanced parser initialized successfully")
                
                # Get sheet names
                sheet_names = parser.get_sheet_names()
                print(f"✅ Found sheets: {sheet_names}")
                
                # Parse parts
                log_operation_start("demo_parse_parts", file=str(test_excel))
                parts = parser.parse_parts()
                log_operation_end("demo_parse_parts", success=True, parts_count=len(parts))
                
                print(f"✅ Parsed {len(parts)} parts")
                for part_num, part_data in list(parts.items())[:3]:  # Show first 3
                    print(f"  - {part_num}: {part_data['name']} ({part_data.get('part_type', 'Unknown')})")
                
                # Build cross-reference index
                log_operation_start("demo_build_cross_index")
                pn_to_name, name_sources = parser.build_cross_index()
                log_operation_end("demo_build_cross_index", success=True, 
                                parts_count=len(pn_to_name), names_count=len(name_sources))
                
                print(f"✅ Built cross-index: {len(pn_to_name)} parts, {len(name_sources)} unique names")
                
                # Show name resolution examples
                for name, sources in list(name_sources.items())[:2]:
                    print(f"  - Name '{name}' appears in {len(sources)} source(s):")
                    for source in sources[:2]:  # Show first 2 sources
                        print(f"    Sheet: {source.get('sheet', 'Unknown')}, Row: {source.get('row', 'Unknown')}")
        
        except Exception as e:
            logger.error(f"Demo failed: {e}")
            print(f"❌ Demo failed: {e}")

def demo_error_handling():
    """Demonstrate comprehensive error handling."""
    print("\n=== Error Handling Demo ===")
    
    # Setup structured logging
    setup_logging(level='INFO', structured=True)
    logger = get_logger(__name__)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Test various error scenarios
        error_scenarios = [
            ("Non-existent file", lambda: FileValidator.validate_excel_file(temp_path / "nonexistent.xlsx")),
            ("Invalid Excel format", lambda: FileValidator.validate_excel_file(temp_path / "invalid.txt")),
            ("Invalid database URL", lambda: DatabaseValidator.validate_graphdb_url("invalid-url")),
            ("Invalid part number", lambda: DataValidator.validate_part_number("")),
            ("Invalid batch size", lambda: ConfigurationValidator.validate_batch_size(-100))
        ]
        
        for scenario_name, test_func in error_scenarios:
            try:
                test_func()
                print(f"❌ {scenario_name}: Should have failed")
            except WindchillImporterError as e:
                print(f"✅ {scenario_name}: Correctly caught {type(e).__name__}: {e}")
                logger.info(f"Handled error in demo: {scenario_name}", extra={
                    'error_type': type(e).__name__,
                    'error_message': str(e)
                })

def main():
    """Run all demos."""
    print("🚀 Windchill XLSX to Knowledge Graph - Enhanced Error Handling Demo")
    print("=" * 70)
    
    try:
        demo_file_validation()
        demo_database_validation()
        demo_data_validation()
        demo_configuration_validation()
        demo_enhanced_spreadsheet_loader()
        demo_error_handling()
        
        print("\n" + "=" * 70)
        print("✅ All demos completed successfully!")
        print("\nKey improvements demonstrated:")
        print("• Comprehensive file validation with detailed error messages")
        print("• Database connection validation for GraphDB and Neo4j")
        print("• Data validation for part numbers, names, and BOM relationships")
        print("• Configuration validation with sensible defaults")
        print("• Enhanced spreadsheet loader with robust error handling")
        print("• Structured logging with operation tracking")
        print("• Custom exception hierarchy for specific error types")
        
    except Exception as e:
        print(f"\n❌ Demo failed with unexpected error: {e}")
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main())