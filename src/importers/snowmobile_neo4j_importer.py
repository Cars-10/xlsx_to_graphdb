#!/usr/bin/env python3
"""
Comprehensive snowmobile data importer for Neo4j with change tracking and relationship mapping.
Loads snowmobile parts, BOM relationships, and change information into Neo4j graph database.
"""

import pandas as pd
import json
import logging
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from core.exceptions import ValidationError, ConfigurationError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SnowmobileNeo4jImporter:
    """Comprehensive importer for snowmobile data into Neo4j with change tracking."""
    
    def __init__(self, uri: str = "bolt://localhost:7687", 
                 user: Optional[str] = None, password: Optional[str] = None):
        try:
            from neo4j import GraphDatabase
        except Exception as e:
            raise RuntimeError(f"Neo4j driver not available: {e}")
        
        # Use the correct authentication format for Neo4j
        if user and password:
            from neo4j import basic_auth
            auth = basic_auth(user, password)
            self.driver = GraphDatabase.driver(uri, auth=auth)
            logger.info(f"Connected to Neo4j at {uri} with authentication")
        else:
            # Try without authentication first
            try:
                self.driver = GraphDatabase.driver(uri)
                logger.info(f"Connected to Neo4j at {uri} without authentication")
            except Exception as e:
                # If that fails, try with default credentials
                try:
                    from neo4j import basic_auth
                    auth = basic_auth("neo4j", "password")
                    self.driver = GraphDatabase.driver(uri, auth=auth)
                    logger.info(f"Connected to Neo4j at {uri} with default credentials")
                except Exception as e2:
                    logger.error(f"Failed to connect to Neo4j: {e2}")
                    raise
        
        self.parts: List[Dict] = []
        self.bom_relationships: List[Dict] = []
        self.change_records: List[Dict] = []
    
    def close(self):
        """Close the Neo4j driver connection."""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j connection closed")
    
    def load_enhanced_data(self, json_file: str = '../../data/processed/snowmobile_enhanced_data.json'):
        """Load enhanced snowmobile data from JSON file."""
        logger.info(f"Loading enhanced data from {json_file}")
        
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            self.parts = data.get('parts', [])
            self.bom_relationships = data.get('bom_relationships', [])
            self.change_records = data.get('change_records', [])
            
            logger.info(f"Loaded {len(self.parts)} parts, {len(self.bom_relationships)} BOM relationships, {len(self.change_records)} change records")
            
        except Exception as e:
            logger.error(f"Failed to load enhanced data: {e}")
            raise ValidationError(f"Failed to load enhanced data: {e}")
    
    def clear_database(self):
        """Clear existing snowmobile data from Neo4j database."""
        logger.info("Clearing existing snowmobile data from Neo4j")
        
        with self.driver.session() as session:
            # Clear all snowmobile-related nodes and relationships
            session.run("MATCH (n:SnowmobilePart) DETACH DELETE n")
            session.run("MATCH (n:SnowmobileChange) DETACH DELETE n")
            session.run("MATCH ()-[r:SNOWMOBILE_RELATION]->() DELETE r")
            
            logger.info("Database cleared of snowmobile data")
    
    def create_parts(self):
        """Create snowmobile parts in Neo4j."""
        logger.info(f"Creating {len(self.parts)} snowmobile parts in Neo4j")
        
        with self.driver.session() as session:
            for i, part in enumerate(self.parts):
                if i % 50 == 0:
                    logger.info(f"Processing part {i+1}/{len(self.parts)}")
                
                # Use MERGE to avoid constraint violations and add snowmobile-specific labels
                query = """
                MERGE (p:Part {number: $number})
                SET p.name = $name,
                    p.type = coalesce(p.type, $type),
                    p.source = coalesce(p.source, $source),
                    p.row_index = $row_index,
                    p.updated_at = datetime()
                WITH p
                SET p:SnowmobilePart
                """
                
                session.run(query, {
                    'number': part.get('number', ''),
                    'name': part.get('name', ''),
                    'type': part.get('type', 'MechanicalPart'),
                    'source': part.get('source', ''),
                    'row_index': part.get('row_index', 0)
                })
        
        logger.info(f"Created/updated {len(self.parts)} snowmobile parts")
    
    def create_changes(self):
        """Create change records in Neo4j."""
        logger.info(f"Creating {len(self.change_records)} change records in Neo4j")
        
        with self.driver.session() as session:
            for i, change in enumerate(self.change_records):
                if i % 50 == 0:
                    logger.info(f"Processing change {i+1}/{len(self.change_records)}")
                
                # Create change node with comprehensive properties
                query = """
                CREATE (c:SnowmobileChange:Change {
                    number: $number,
                    name: $name,
                    type: $type,
                    state: $state,
                    priority: $priority,
                    description: $description,
                    need_date: $need_date,
                    create_date: $create_date,
                    creator: $creator,
                    affected_part_number: $affected_part_number,
                    affected_part_name: $affected_part_name,
                    created_at: datetime()
                })
                """
                
                session.run(query, {
                    'number': change.get('number', ''),
                    'name': change.get('name', ''),
                    'type': change.get('type', 'ECO'),
                    'state': change.get('state', 'OPEN'),
                    'priority': change.get('priority', 'MEDIUM'),
                    'description': change.get('description', ''),
                    'need_date': change.get('need_date', datetime.now().strftime('%Y-%m-%d')),
                    'create_date': change.get('create_date', datetime.now().strftime('%Y-%m-%d')),
                    'creator': change.get('creator', 'System'),
                    'affected_part_number': change.get('affected_part_number', ''),
                    'affected_part_name': change.get('affected_part_name', '')
                })
        
        logger.info(f"Created {len(self.change_records)} change records")
    
    def create_bom_relationships(self):
        """Create BOM relationships between parts."""
        logger.info(f"Creating {len(self.bom_relationships)} BOM relationships")
        
        with self.driver.session() as session:
            for i, rel in enumerate(self.bom_relationships):
                if i % 100 == 0:
                    logger.info(f"Processing BOM relationship {i+1}/{len(self.bom_relationships)}")
                
                # Create HAS_COMPONENT relationship
                query = """
                MATCH (parent:SnowmobilePart {name: $parent_name})
                MATCH (child:SnowmobilePart {name: $child_name})
                CREATE (parent)-[r:HAS_COMPONENT {
                    relationship_type: $relationship_type,
                    source: $source,
                    row_index: $row_index,
                    created_at: datetime()
                }]->(child)
                """
                
                try:
                    result = session.run(query, {
                        'parent_name': rel.get('parent_name', ''),
                        'child_name': rel.get('child_name', ''),
                        'relationship_type': rel.get('relationship_type', 'HAS_COMPONENT'),
                        'source': rel.get('source', ''),
                        'row_index': rel.get('row_index', 0)
                    })
                    
                    if result.consume().counters.relationships_created == 0:
                        logger.warning(f"Failed to create BOM relationship: {rel.get('parent_name')} -> {rel.get('child_name')}")
                        
                except Exception as e:
                    logger.warning(f"Error creating BOM relationship: {e}")
                    continue
        
        logger.info(f"Created BOM relationships")
    
    def create_change_relationships(self):
        """Create relationships between changes and affected parts."""
        logger.info("Creating change relationships")
        
        with self.driver.session() as session:
            for i, change in enumerate(self.change_records):
                if i % 50 == 0:
                    logger.info(f"Processing change relationship {i+1}/{len(self.change_records)}")
                
                # Create AFFECTS_PART relationship
                query = """
                MATCH (c:SnowmobileChange {number: $change_number})
                MATCH (p:SnowmobilePart {number: $part_number})
                CREATE (c)-[r:AFFECTS_PART {
                    change_type: $change_type,
                    state: $state,
                    priority: $priority,
                    created_at: datetime()
                }]->(p)
                """
                
                try:
                    session.run(query, {
                        'change_number': change.get('number', ''),
                        'part_number': change.get('affected_part_number', ''),
                        'change_type': change.get('type', 'ECO'),
                        'state': change.get('state', 'OPEN'),
                        'priority': change.get('priority', 'MEDIUM')
                    })
                except Exception as e:
                    logger.warning(f"Error creating change relationship for {change.get('number')}: {e}")
                    continue
        
        logger.info("Created change relationships")
    
    def create_part_relationships(self):
        """Create additional part relationships based on naming patterns."""
        logger.info("Creating additional part relationships")
        
        with self.driver.session() as session:
            # Create SUPERSEDES relationships for parts with similar names but different numbers
            query = """
            MATCH (p1:SnowmobilePart), (p2:SnowmobilePart)
            WHERE p1 <> p2 
            AND p1.name = p2.name 
            AND p1.number <> p2.number
            AND p1.number < p2.number
            CREATE (p2)-[r:SUPERSEDES {
                relationship_type: 'SUPERSEDES',
                created_at: datetime()
            }]->(p1)
            """
            
            result = session.run(query)
            superseded_count = result.consume().counters.relationships_created
            logger.info(f"Created {superseded_count} SUPERSEDES relationships")
            
            # Create PART_OF relationships for components with similar prefixes
            query = """
            MATCH (p1:SnowmobilePart), (p2:SnowmobilePart)
            WHERE p1 <> p2 
            AND p1.number STARTS WITH left(p2.number, 3)
            AND p1.number <> p2.number
            AND NOT (p1)-[:HAS_COMPONENT]-(p2)
            AND NOT (p2)-[:HAS_COMPONENT]-(p1)
            CREATE (p1)-[r:PART_OF {
                relationship_type: 'PART_OF',
                created_at: datetime()
            }]->(p2)
            """
            
            result = session.run(query)
            part_of_count = result.consume().counters.relationships_created
            logger.info(f"Created {part_of_count} PART_OF relationships")
    
    def create_change_tracking_graph(self):
        """Create comprehensive change tracking relationships."""
        logger.info("Creating change tracking graph")
        
        with self.driver.session() as session:
            # Create DEPENDS_ON relationships between changes affecting the same part
            query = """
            MATCH (c1:SnowmobileChange), (c2:SnowmobileChange)
            WHERE c1 <> c2 
            AND c1.affected_part_number = c2.affected_part_number
            AND c1.create_date < c2.create_date
            AND NOT (c1)-[:DEPENDS_ON]-(c2)
            AND NOT (c2)-[:DEPENDS_ON]-(c1)
            CREATE (c2)-[r:DEPENDS_ON {
                relationship_type: 'DEPENDS_ON',
                created_at: datetime()
            }]->(c1)
            """
            
            result = session.run(query)
            depends_count = result.consume().counters.relationships_created
            logger.info(f"Created {depends_count} DEPENDS_ON relationships between changes")
            
            # Create RELATED_TO relationships between changes of same type
            query = """
            MATCH (c1:SnowmobileChange), (c2:SnowmobileChange)
            WHERE c1 <> c2 
            AND c1.type = c2.type
            AND c1.create_date < c2.create_date
            AND NOT (c1)-[:RELATED_TO]-(c2)
            AND NOT (c2)-[:RELATED_TO]-(c1)
            AND NOT (c1)-[:DEPENDS_ON]-(c2)
            AND NOT (c2)-[:DEPENDS_ON]-(c1)
            CREATE (c2)-[r:RELATED_TO {
                relationship_type: 'RELATED_TO',
                change_type: c1.type,
                created_at: datetime()
            }]->(c1)
            """
            
            result = session.run(query)
            related_count = result.consume().counters.relationships_created
            logger.info(f"Created {related_count} RELATED_TO relationships between changes")
    
    def create_indexes(self):
        """Create indexes for better query performance."""
        logger.info("Creating indexes for better performance")
        
        with self.driver.session() as session:
            indexes = [
                "CREATE INDEX snowmobile_part_number IF NOT EXISTS FOR (p:SnowmobilePart) ON (p.number)",
                "CREATE INDEX snowmobile_part_name IF NOT EXISTS FOR (p:SnowmobilePart) ON (p.name)",
                "CREATE INDEX snowmobile_change_number IF NOT EXISTS FOR (c:SnowmobileChange) ON (c.number)",
                "CREATE INDEX snowmobile_change_type IF NOT EXISTS FOR (c:SnowmobileChange) ON (c.type)",
                "CREATE INDEX snowmobile_change_state IF NOT EXISTS FOR (c:SnowmobileChange) ON (c.state)"
            ]
            
            for index_query in indexes:
                try:
                    session.run(index_query)
                    logger.info(f"Created index: {index_query}")
                except Exception as e:
                    logger.warning(f"Index may already exist or error creating: {e}")
    
    def verify_import(self) -> Dict:
        """Verify the import by querying the database."""
        logger.info("Verifying snowmobile data import")
        
        with self.driver.session() as session:
            # Count parts
            parts_result = session.run("MATCH (p:SnowmobilePart) RETURN count(p) as count")
            parts_count = parts_result.single()['count']
            
            # Count changes
            changes_result = session.run("MATCH (c:SnowmobileChange) RETURN count(c) as count")
            changes_count = changes_result.single()['count']
            
            # Count relationships
            rels_result = session.run("MATCH ()-[r:HAS_COMPONENT]->() RETURN count(r) as count")
            bom_rels_count = rels_result.single()['count']
            
            change_rels_result = session.run("MATCH ()-[r:AFFECTS_PART]->() RETURN count(r) as count")
            change_rels_count = change_rels_result.single()['count']
            
            # Sample data
            sample_parts = session.run("MATCH (p:SnowmobilePart) RETURN p.number, p.name LIMIT 5").data()
            sample_changes = session.run("MATCH (c:SnowmobileChange) RETURN c.number, c.type, c.state LIMIT 5").data()
            
            verification = {
                'parts_count': parts_count,
                'changes_count': changes_count,
                'bom_relationships_count': bom_rels_count,
                'change_relationships_count': change_rels_count,
                'total_nodes': parts_count + changes_count,
                'total_relationships': bom_rels_count + change_rels_count,
                'sample_parts': sample_parts,
                'sample_changes': sample_changes
            }
            
            logger.info(f"Verification completed:")
            logger.info(f"- Parts: {parts_count}")
            logger.info(f"- Changes: {changes_count}")
            logger.info(f"- BOM Relationships: {bom_rels_count}")
            logger.info(f"- Change Relationships: {change_rels_count}")
            
            return verification
    
    def run_comprehensive_import(self):
        """Run the complete import process."""
        logger.info("Starting comprehensive snowmobile import to Neo4j")
        
        try:
            # Load data
            self.load_enhanced_data()
            
            # Clear existing data
            self.clear_database()
            
            # Create indexes
            self.create_indexes()
            
            # Create nodes
            self.create_parts()
            self.create_changes()
            
            # Create relationships
            self.create_bom_relationships()
            self.create_change_relationships()
            self.create_part_relationships()
            self.create_change_tracking_graph()
            
            # Verify import
            verification = self.verify_import()
            
            logger.info("Comprehensive snowmobile import completed successfully!")
            return verification
            
        except Exception as e:
            logger.error(f"Import failed: {e}")
            raise

def main():
    """Main function to run the comprehensive snowmobile import."""
    logger.info("Starting snowmobile Neo4j import process")
    
    # Initialize importer with correct credentials
    importer = SnowmobileNeo4jImporter(
        uri="bolt://localhost:7687",
        user="neo4j", 
        password="tstpwdpwd"
    )
    
    try:
        # Run comprehensive import
        verification = importer.run_comprehensive_import()
        
        # Save verification results
        with open('../../data/processed/snowmobile_import_verification.json', 'w') as f:
            json.dump(verification, f, indent=2, default=str)
        
        logger.info("Import verification saved to snowmobile_import_verification.json")
        
        # Print summary
        print("\n" + "="*60)
        print("SNOWMOBILE DATA IMPORT SUMMARY")
        print("="*60)
        print(f"Parts imported: {verification['parts_count']}")
        print(f"Changes imported: {verification['changes_count']}")
        print(f"BOM relationships: {verification['bom_relationships_count']}")
        print(f"Change relationships: {verification['change_relationships_count']}")
        print(f"Total nodes: {verification['total_nodes']}")
        print(f"Total relationships: {verification['total_relationships']}")
        print("\nSample parts:")
        for part in verification['sample_parts']:
            print(f"  - {part['p.number']}: {part['p.name']}")
        print("\nSample changes:")
        for change in verification['sample_changes']:
            print(f"  - {change['c.number']}: {change['c.type']} ({change['c.state']})")
        print("="*60)
        
    except Exception as e:
        logger.error(f"Import process failed: {e}")
        sys.exit(1)
    
    finally:
        importer.close()

if __name__ == "__main__":
    main()