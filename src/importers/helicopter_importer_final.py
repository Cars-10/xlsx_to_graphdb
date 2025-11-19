#!/usr/bin/env python3
"""
Custom Helicopter Importer for Neo4j with Change Tracking
Works with the actual data structure from the helicopter files
"""

import pandas as pd
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Any

try:
    from neo4j import GraphDatabase
except ImportError:
    print("neo4j-driver not found. Install with: pip install neo4j")
    sys.exit(1)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HelicopterImporter:
    """Custom importer for helicopter data with change tracking"""
    
    def __init__(self, uri: str, user: str, password: str):
        """Initialize importer with Neo4j connection"""
        self.driver = None
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            logger.info("Connected to Neo4j database")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            raise
    
    def close(self):
        """Close Neo4j connection"""
        if self.driver:
            self.driver.close()
            logger.info("Closed Neo4j connection")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def import_helicopter_data(self, excel_path: str, bom_path: str) -> Dict[str, Any]:
        """Import helicopter data with change tracking"""
        logger.info("Starting helicopter data import")
        
        try:
            # Load and process data
            parts_data = self._load_helicopter_parts(excel_path)
            bom_data = self._load_bom_relationships(bom_path)
            
            logger.info(f"Loaded {len(parts_data)} helicopter parts")
            logger.info(f"Loaded {len(bom_data)} BOM relationships")
            
            # Import into Neo4j
            with self.driver.session() as session:
                parts_created = session.execute_write(self._create_parts, parts_data)
                relationships_created = session.execute_write(self._create_relationships, bom_data)
                changes_created = session.execute_write(self._create_change_records, parts_data)
            
            result = {
                "parts_created": parts_created,
                "relationships_created": relationships_created,
                "changes_created": changes_created,
                "total_nodes": parts_created + changes_created,
                "total_relationships": relationships_created
            }
            
            logger.info("Helicopter import completed successfully")
            return result
            
        except Exception as e:
            logger.error(f"Import failed: {e}")
            raise
    
    def _load_helicopter_parts(self, excel_path: str) -> List[Dict[str, Any]]:
        """Load helicopter parts from Excel file"""
        logger.info("Loading helicopter parts from Excel")
        
        parts = []
        excel_file = pd.ExcelFile(excel_path)
        
        # Process MechanicalPart-Sheet (main sheet with helicopter data)
        if 'MechanicalPart-Sheet' in excel_file.sheet_names:
            df = pd.read_excel(excel_path, sheet_name='MechanicalPart-Sheet')
            
            # Find the header row (row 3 based on analysis)
            if len(df) > 3:
                headers = df.iloc[3].tolist()
                data_df = df.iloc[4:].copy()
                data_df.columns = headers
                
                logger.info(f"Processing {len(data_df)} rows from MechanicalPart-Sheet")
                
                # Filter for helicopter parts
                for _, row in data_df.iterrows():
                    part_data = row.to_dict()
                    
                    # Check if it's a helicopter part
                    is_helicopter = False
                    if pd.notna(part_data.get('Name', '')):
                        name = str(part_data['Name']).lower()
                        if 'helicopter' in name or 'heli' in name:
                            is_helicopter = True
                    
                    if pd.notna(part_data.get('Number', '')):
                        number = str(part_data['Number'])
                        if any(pattern in number for pattern in ['HEL', 'HELI', '600']):
                            is_helicopter = True
                    
                    # Include all parts but mark helicopter ones
                    part_data['_is_helicopter'] = is_helicopter
                    parts.append(part_data)
        
        logger.info(f"Found {len(parts)} total parts, {sum(1 for p in parts if p['_is_helicopter'])} helicopter parts")
        return parts
    
    def _load_bom_relationships(self, bom_path: str) -> List[Dict[str, str]]:
        """Load BOM relationships from CSV"""
        logger.info("Loading BOM relationships")
        
        df = pd.read_csv(bom_path)
        relationships = df.to_dict('records')
        
        logger.info(f"Loaded {len(relationships)} BOM relationships")
        return relationships
    
    def _create_parts(self, tx, parts_data: List[Dict[str, Any]]) -> int:
        """Create parts in Neo4j"""
        created_count = 0
        
        for part in parts_data:
            part_number = str(part.get('Number', '')).strip()
            part_name = str(part.get('Name', '')).strip()
            
            if not part_number or part_number == 'nan':
                continue
            
            # Create part node
            query = """
            MERGE (p:Part {number: $number})
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
                p.is_helicopter = $is_helicopter,
                p.created_at = datetime()
            RETURN p
            """
            
            result = tx.run(query,
                          number=part_number,
                          name=part_name,
                          type=str(part.get('Type', '')),
                          end_item=str(part.get('End Item', '')),
                          phantom=str(part.get('Phantom', '')),
                          trace_code=str(part.get('Trace Code', '')),
                          generic_type=str(part.get('Generic Type', '')),
                          serviceable=str(part.get('Serviceable', '')),
                          assembly_mode=str(part.get('Assembly Mode', '')),
                          location=str(part.get('Location', '')),
                          organization_id=str(part.get('Organization ID', '')),
                          revision=str(part.get('Revision', '')),
                          view=str(part.get('View', '')),
                          state=str(part.get('State', '')),
                          lifecycle=str(part.get('Lifecycle', '')),
                          source=str(part.get('Source', '')),
                          default_unit=str(part.get('Default Unit', '')),
                          material=str(part.get('Material', '')),
                          part_classification=str(part.get('Part Classification', '')),
                          is_helicopter=part.get('_is_helicopter', False)
                          )
            
            if result.single():
                created_count += 1
        
        logger.info(f"Created {created_count} parts in Neo4j")
        return created_count
    
    def _create_relationships(self, tx, bom_data: List[Dict[str, str]]) -> int:
        """Create BOM relationships in Neo4j"""
        created_count = 0
        
        for rel in bom_data:
            parent_name = str(rel.get('Parent Name', '')).strip()
            child_name = str(rel.get('Child Name', '')).strip()
            
            if not parent_name or not child_name or parent_name == 'nan' or child_name == 'nan':
                continue
            
            query = """
            MATCH (parent:Part {number: $parent_number})
            MATCH (child:Part {number: $child_number})
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
        
        logger.info(f"Created {created_count} relationships in Neo4j")
        return created_count
    
    def _create_change_records(self, tx, parts_data: List[Dict[str, Any]]) -> int:
        """Create change records for parts with revision/state information"""
        created_count = 0
        
        for part in parts_data:
            part_number = str(part.get('Number', '')).strip()
            revision = str(part.get('Revision', '')).strip()
            state = str(part.get('State', '')).strip()
            
            if not part_number or part_number == 'nan':
                continue
            
            # Only create change records if there's actual change data
            if revision and revision != 'nan':
                change_id = f"CHANGE_{part_number}_{revision}"
                
                query = """
                MERGE (c:ChangeRecord {change_id: $change_id})
                SET c.part_number = $part_number,
                    c.revision = $revision,
                    c.state = $state,
                    c.created_at = datetime()
                RETURN c
                """
                
                result = tx.run(query,
                              change_id=change_id,
                              part_number=part_number,
                              revision=revision,
                              state=state
                              )
                
                if result.single():
                    created_count += 1
                    
                    # Link change to part
                    link_query = """
                    MATCH (c:ChangeRecord {change_id: $change_id})
                    MATCH (p:Part {number: $part_number})
                    MERGE (c)-[r:AFFECTS_PART]->(p)
                    SET r.created_at = datetime()
                    RETURN r
                    """
                    
                    tx.run(link_query, change_id=change_id, part_number=part_number)
        
        logger.info(f"Created {created_count} change records in Neo4j")
        return created_count

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Import helicopter data with change tracking into Neo4j")
    parser.add_argument("--excel", default="data/Helicopter.xlsx", help="Path to helicopter Excel file")
    parser.add_argument("--bom", default="data/Helicopter_bom.csv", help="Path to helicopter BOM CSV file")
    parser.add_argument("--uri", default="bolt://localhost:7687", help="Neo4j Bolt URI")
    parser.add_argument("--user", default="neo4j", help="Neo4j username")
    parser.add_argument("--password", default=None, help="Neo4j password (or set NEO4J_PASSWORD env)")
    
    args = parser.parse_args()
    
    # Get password from environment or argument
    password = args.password or os.environ.get("NEO4J_PASSWORD")
    if not password:
        logger.error("Neo4j password not provided. Set NEO4J_PASSWORD environment variable or use --password")
        return 1
    
    try:
        with HelicopterImporter(args.uri, args.user, password) as importer:
            result = importer.import_helicopter_data(args.excel, args.bom)
            
            print(f"\n=== IMPORT RESULTS ===")
            print(f"Parts created: {result['parts_created']}")
            print(f"Relationships created: {result['relationships_created']}")
            print(f"Change records created: {result['changes_created']}")
            print(f"Total nodes: {result['total_nodes']}")
            print(f"Total relationships: {result['total_relationships']}")
            
            return 0
            
    except Exception as e:
        logger.error(f"Import failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())