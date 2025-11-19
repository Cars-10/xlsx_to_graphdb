#!/usr/bin/env python3
"""
Enhanced Helicopter Importer with Change Tracking for Neo4j
This module imports helicopter parts, BOM relationships, and change information into Neo4j
"""

import pandas as pd
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path
import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from exceptions import ValidationError, DatabaseConnectionError
from validation import FileValidator, DataValidator
from logging_config import setup_logging, get_logger, log_operation_start, log_operation_end

try:
    from neo4j import GraphDatabase
except ImportError:
    print("neo4j-driver not found. Install with: pip install neo4j")
    sys.exit(1)

# Setup logging
setup_logging(level="INFO", structured=False, include_console=True)
logger = get_logger(__name__)

class HelicopterChangeImporter:
    """Importer for helicopter data with change tracking"""
    
    def __init__(self, uri: str, user: str, password: str):
        """Initialize importer with Neo4j connection"""
        self.driver = None
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            logger.info("Connected to Neo4j database")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise DatabaseConnectionError(f"Neo4j connection failed: {e}", database_type="neo4j", url=uri)
    
    def close(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
            logger.info("Closed Neo4j connection")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def load_helicopter_data(self, excel_path: str, bom_path: str) -> Dict[str, Any]:
        """Load helicopter data with change tracking"""
        log_operation_start("load_helicopter_data", excel_path=excel_path, bom_path=bom_path)
        
        try:
            # Validate input files
            file_validator = FileValidator()
            file_validator.validate_excel_file(excel_path)
            file_validator.validate_csv_file(bom_path)
            
            # Load and process data
            parts_data = self._extract_helicopter_parts(excel_path)
            bom_data = self._extract_bom_relationships(bom_path)
            change_data = self._extract_change_information(excel_path)
            
            # Load into Neo4j
            with self.driver.session() as session:
                # Create parts with change information
                parts_created = session.execute_write(self._create_helicopter_parts, parts_data)
                
                # Create BOM relationships
                relationships_created = session.execute_write(self._create_bom_relationships, bom_data)
                
                # Create change records and relationships
                changes_created = session.execute_write(self._create_change_records, change_data)
                
                # Link changes to parts
                change_links_created = session.execute_write(self._link_changes_to_parts, change_data)
            
            result = {
                "parts_created": parts_created,
                "relationships_created": relationships_created,
                "changes_created": changes_created,
                "change_links_created": change_links_created,
                "total_nodes": parts_created + changes_created,
                "total_relationships": relationships_created + change_links_created
            }
            
            log_operation_end("load_helicopter_data", result=result)
            return result
            
        except Exception as e:
            logger.error(f"Failed to load helicopter data: {e}")
            log_operation_end("load_helicopter_data", error=str(e), success=False)
            raise
    
    def _extract_helicopter_parts(self, excel_path: str) -> List[Dict[str, Any]]:
        """Extract helicopter parts from Excel file"""
        logger.info("Extracting helicopter parts from Excel")
        
        parts = []
        excel_file = pd.ExcelFile(excel_path)
        
        # Process relevant sheets
        relevant_sheets = ['MechanicalPart-Sheet', 'WTPart-Sheet', 'Helicopter-Sheet']
        
        for sheet_name in relevant_sheets:
            if sheet_name in excel_file.sheet_names:
                df = pd.read_excel(excel_path, sheet_name=sheet_name)
                
                # Clean data - skip header rows that contain metadata
                df_clean = df.dropna(how='all')
                if len(df_clean) > 3:
                    # Find the row with actual headers (skip import metadata)
                    header_row_idx = None
                    for i, row in df_clean.iterrows():
                        if 'Number' in str(row.values) and 'Name' in str(row.values):
                            header_row_idx = i
                            break
                    
                    if header_row_idx is not None:
                        # Use the identified header row
                        headers = df_clean.iloc[header_row_idx].tolist()
                        df_data = df_clean.iloc[header_row_idx + 1:].copy()
                        df_data.columns = headers
                        
                        # Filter for helicopter parts
                        helicopter_parts = self._identify_helicopter_parts(df_data)
                        parts.extend(helicopter_parts)
                    else:
                        logger.warning(f"Could not find header row in {sheet_name}")
        
        logger.info(f"Extracted {len(parts)} helicopter parts")
        return parts
    
    def _identify_helicopter_parts(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Identify helicopter parts from dataframe"""
        helicopter_parts = []
        
        # Filter by name containing helicopter terms
        if 'Name' in df.columns:
            name_mask = df['Name'].str.contains('helicopter|Helicopter|HELI', na=False, case=False)
            helicopter_parts.extend(df[name_mask].to_dict('records'))
        
        # Filter by part number containing helicopter patterns
        if 'Number' in df.columns:
            number_mask = df['Number'].str.contains('HEL|HELI|600', na=False, case=False)
            helicopter_parts.extend(df[number_mask].to_dict('records'))
        
        # Remove duplicates
        seen = set()
        unique_parts = []
        for part in helicopter_parts:
            part_key = str(part.get('Number', '')) + str(part.get('Name', ''))
            if part_key not in seen:
                seen.add(part_key)
                unique_parts.append(part)
        
        return unique_parts
    
    def _extract_bom_relationships(self, bom_path: str) -> List[Dict[str, str]]:
        """Extract BOM relationships from CSV"""
        logger.info("Extracting BOM relationships")
        
        df = pd.read_csv(bom_path)
        relationships = df.to_dict('records')
        
        logger.info(f"Extracted {len(relationships)} BOM relationships")
        return relationships
    
    def _extract_change_information(self, excel_path: str) -> List[Dict[str, Any]]:
        """Extract change information from Excel file"""
        logger.info("Extracting change information")
        
        changes = []
        excel_file = pd.ExcelFile(excel_path)
        
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
            
            # Clean data
            df_clean = df.dropna(how='all')
            if len(df_clean) > 3:
                # Find the row with actual headers
                header_row_idx = None
                for i, row in df_clean.iterrows():
                    if 'Number' in str(row.values) and 'Name' in str(row.values):
                        header_row_idx = i
                        break
                
                if header_row_idx is not None:
                    headers = df_clean.iloc[header_row_idx].tolist()
                    df_data = df_clean.iloc[header_row_idx + 1:].copy()
                    df_data.columns = headers
                    
                    # Look for change-related columns
                    change_columns = [col for col in df_data.columns if any(keyword in str(col) for keyword in ['Change', 'Revision', 'Version', 'State'])]
                    
                    if change_columns:
                        change_data = df_data[change_columns].dropna(how='all')
                        if not change_data.empty:
                            # Add metadata
                            for record in change_data.to_dict('records'):
                                record['_source_sheet'] = sheet_name
                                record['_part_number'] = record.get('Number', '')
                                record['_part_name'] = record.get('Name', '')
                                changes.append(record)
        
        logger.info(f"Extracted {len(changes)} change records")
        return changes
    
    def _create_helicopter_parts(self, tx, parts_data: List[Dict[str, Any]]) -> int:
        """Create helicopter parts in Neo4j"""
        created_count = 0
        
        for part in parts_data:
            part_number = str(part.get('Number', '')).strip()
            part_name = str(part.get('Name', '')).strip()
            part_type = str(part.get('Type', '')).strip()
            
            if not part_number:
                continue
            
            # Create part node with properties
            query = """
            MERGE (p:HelicopterPart {number: $number})
            SET p.name = $name,
                p.type = $type,
                p.end_item = $end_item,
                p.phantom = $phantom,
                p.trace_code = $trace_code,
                p.generic_type = $generic_type,
                p.serviceable = $serviceable,
                p.assembly_mode = $assembly_mode,
                p.location = $location,
                p.organization_id = $organization_id,
                p.revision = $revision,
                p.view = $view,
                p.state = $state,
                p.lifecycle = $lifecycle,
                p.source = $source,
                p.default_unit = $default_unit,
                p.material = $material,
                p.part_classification = $part_classification,
                p.created_at = datetime()
            RETURN p
            """
            
            result = tx.run(query, 
                          number=part_number,
                          name=part_name,
                          type=part_type,
                          end_item=part.get('End Item', ''),
                          phantom=part.get('Phantom', ''),
                          trace_code=part.get('Trace Code', ''),
                          generic_type=part.get('Generic Type', ''),
                          serviceable=part.get('Serviceable', ''),
                          assembly_mode=part.get('Assembly Mode', ''),
                          location=part.get('Location', ''),
                          organization_id=part.get('Organization ID', ''),
                          revision=part.get('Revision', ''),
                          view=part.get('View', ''),
                          state=part.get('State', ''),
                          lifecycle=part.get('Lifecycle', ''),
                          source=part.get('Source', ''),
                          default_unit=part.get('Default Unit', ''),
                          material=part.get('Material', ''),
                          part_classification=part.get('Part Classification', '')
                          )
            
            if result.single():
                created_count += 1
        
        logger.info(f"Created {created_count} helicopter parts in Neo4j")
        return created_count
    
    def _create_bom_relationships(self, tx, bom_data: List[Dict[str, str]]) -> int:
        """Create BOM relationships in Neo4j"""
        created_count = 0
        
        for rel in bom_data:
            parent_name = str(rel.get('Parent Name', '')).strip()
            child_name = str(rel.get('Child Name', '')).strip()
            
            if not parent_name or not child_name:
                continue
            
            query = """
            MATCH (parent:HelicopterPart {number: $parent_number})
            MATCH (child:HelicopterPart {number: $child_number})
            MERGE (parent)-[r:HAS_COMPONENT]->(child)
            SET r.created_at = datetime()
            RETURN r
            """
            
            result = tx.run(query, 
                          parent_number=parent_name,
                          child_number=child_name
                          )
            
            if result.single():
                created_count += 1
        
        logger.info(f"Created {created_count} BOM relationships in Neo4j")
        return created_count
    
    def _create_change_records(self, tx, change_data: List[Dict[str, Any]]) -> int:
        """Create change records in Neo4j"""
        created_count = 0
        
        for change in change_data:
            # Generate change ID
            change_id = f"CHANGE_{hash(str(change)) % 1000000}"
            
            query = """
            MERGE (c:ChangeRecord {change_id: $change_id})
            SET c.revision = $revision,
                c.state = $state,
                c.source_sheet = $source_sheet,
                c.part_number = $part_number,
                c.part_name = $part_name,
                c.created_at = datetime()
            RETURN c
            """
            
            result = tx.run(query,
                          change_id=change_id,
                          revision=str(change.get('Revision', '')),
                          state=str(change.get('State', '')),
                          source_sheet=change.get('_source_sheet', ''),
                          part_number=change.get('_part_number', ''),
                          part_name=change.get('_part_name', '')
                          )
            
            if result.single():
                created_count += 1
        
        logger.info(f"Created {created_count} change records in Neo4j")
        return created_count
    
    def _link_changes_to_parts(self, tx, change_data: List[Dict[str, Any]]) -> int:
        """Link change records to affected parts"""
        linked_count = 0
        
        for change in change_data:
            part_number = change.get('_part_number', '')
            if not part_number:
                continue
            
            change_id = f"CHANGE_{hash(str(change)) % 1000000}"
            
            query = """
            MATCH (c:ChangeRecord {change_id: $change_id})
            MATCH (p:HelicopterPart {number: $part_number})
            MERGE (c)-[r:AFFECTS_PART]->(p)
            SET r.created_at = datetime()
            RETURN r
            """
            
            result = tx.run(query,
                          change_id=change_id,
                          part_number=part_number
                          )
            
            if result.single():
                linked_count += 1
        
        logger.info(f"Linked {linked_count} changes to parts in Neo4j")
        return linked_count

def main():
    """Main function to run helicopter import with change tracking"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Import helicopter data with change tracking into Neo4j")
    parser.add_argument("--excel", default="data/Helicopter.xlsx", help="Path to helicopter Excel file")
    parser.add_argument("--bom", default="data/Helicopter_bom.csv", help="Path to helicopter BOM CSV file")
    parser.add_argument("--uri", default="bolt://localhost:7687", help="Neo4j Bolt URI")
    parser.add_argument("--user", default="neo4j", help="Neo4j username")
    parser.add_argument("--password", default=None, help="Neo4j password (or set NEO4J_PASSWORD env)")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    args = parser.parse_args()
    
    # Set up logging
    setup_logging(level=args.log_level.upper(), structured=False, include_console=True)
    
    # Get password from environment or argument
    password = args.password or os.environ.get("NEO4J_PASSWORD")
    if not password:
        logger.error("Neo4j password not provided. Set NEO4J_PASSWORD environment variable or use --password")
        return 1
    
    try:
        # Run import
        with HelicopterChangeImporter(args.uri, args.user, password) as importer:
            result = importer.load_helicopter_data(args.excel, args.bom)
            
            print(f"\n=== Import Complete ===")
            print(f"Parts created: {result['parts_created']}")
            print(f"BOM relationships created: {result['relationships_created']}")
            print(f"Change records created: {result['changes_created']}")
            print(f"Change-to-part links created: {result['change_links_created']}")
            print(f"Total nodes: {result['total_nodes']}")
            print(f"Total relationships: {result['total_relationships']}")
            
            return 0
            
    except Exception as e:
        logger.error(f"Import failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())