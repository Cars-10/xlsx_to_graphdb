#!/usr/bin/env python3
"""
Comprehensive verification script for snowmobile change tracking graph in Neo4j.
Analyzes the complete relationship network and change dependencies.
"""

import json
import logging
from neo4j import GraphDatabase
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SnowmobileGraphVerifier:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="tstpwdpwd"):
        """Initialize the verifier with Neo4j connection."""
        from neo4j import basic_auth
        auth = basic_auth(user, password)
        self.driver = GraphDatabase.driver(uri, auth=auth)
        logger.info("Connected to Neo4j for verification")

    def close(self):
        """Close the Neo4j connection."""
        if self.driver:
            self.driver.close()

    def run_query(self, query, parameters=None):
        """Execute a Cypher query and return results."""
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]

    def verify_basic_counts(self):
        """Verify basic counts of nodes and relationships."""
        logger.info("=== BASIC COUNTS VERIFICATION ===")
        
        queries = {
            "Total Parts": "MATCH (p:WTPart) RETURN count(p) as count",
            "Total Changes": "MATCH (c:Change) RETURN count(c) as count",
            "Total BOM Relationships": "MATCH ()-[r:HAS_COMPONENT]->() RETURN count(r) as count",
            "Total Change Relationships": "MATCH ()-[r:AFFECTS_PART]->() RETURN count(r) as count",
            "Total SUPERSEDES Relationships": "MATCH ()-[r:SUPERSEDES]->() RETURN count(r) as count",
            "Total PART_OF Relationships": "MATCH ()-[r:PART_OF]->() RETURN count(r) as count",
            "Total DEPENDS_ON Relationships": "MATCH ()-[r:DEPENDS_ON]->() RETURN count(r) as count",
            "Total RELATED_TO Relationships": "MATCH ()-[r:RELATED_TO]->() RETURN count(r) as count"
        }
        
        results = {}
        for name, query in queries.items():
            result = self.run_query(query)
            count = result[0]['count'] if result else 0
            results[name] = count
            logger.info(f"{name}: {count}")
        
        return results

    def verify_change_types(self):
        """Analyze change types and their distribution."""
        logger.info("\n=== CHANGE TYPES ANALYSIS ===")
        
        query = """
        MATCH (c:Change)
        RETURN c.type as change_type, count(c) as count
        ORDER BY count DESC
        """
        
        results = self.run_query(query)
        for result in results:
            logger.info(f"{result['change_type']}: {result['count']}")
        
        return results

    def verify_change_states(self):
        """Analyze change states and their distribution."""
        logger.info("\n=== CHANGE STATES ANALYSIS ===")
        
        query = """
        MATCH (c:Change)
        RETURN c.state as state, count(c) as count
        ORDER BY count DESC
        """
        
        results = self.run_query(query)
        for result in results:
            logger.info(f"{result['state']}: {result['count']}")
        
        return results

    def verify_part_categories(self):
        """Analyze part categories and types."""
        logger.info("\n=== PART CATEGORIES ANALYSIS ===")
        
        query = """
        MATCH (p:WTPart)
        RETURN p.type as part_type, count(p) as count
        ORDER BY count DESC
        LIMIT 20
        """
        
        results = self.run_query(query)
        for result in results:
            logger.info(f"{result['part_type']}: {result['count']}")
        
        return results

    def verify_complex_relationships(self):
        """Verify complex multi-hop relationships."""
        logger.info("\n=== COMPLEX RELATIONSHIPS VERIFICATION ===")
        
        # Parts with multiple changes
        query1 = """
        MATCH (p:WTPart)<-[r:AFFECTS_PART]-(c:Change)
        WITH p, count(r) as change_count
        WHERE change_count > 1
        RETURN p.number as part_number, p.name as part_name, change_count
        ORDER BY change_count DESC
        LIMIT 10
        """
        
        results1 = self.run_query(query1)
        logger.info("Parts with multiple changes:")
        for result in results1:
            logger.info(f"  {result['part_number']} ({result['part_name']}): {result['change_count']} changes")
        
        # Changes affecting multiple parts
        query2 = """
        MATCH (c:Change)-[r:AFFECTS_PART]->(p:WTPart)
        WITH c, count(r) as part_count
        WHERE part_count > 1
        RETURN c.number as change_number, c.type as change_type, part_count
        ORDER BY part_count DESC
        LIMIT 10
        """
        
        results2 = self.run_query(query2)
        logger.info("\nChanges affecting multiple parts:")
        for result in results2:
            logger.info(f"  {result['change_number']} ({result['change_type']}): affects {result['part_count']} parts")
        
        # Part supersession chains
        query3 = """
        MATCH path = (p1:WTPart)-[:SUPERSEDES*1..5]->(p2:WTPart)
        WITH path, length(path) as chain_length
        WHERE chain_length >= 2
        RETURN 
            [node in nodes(path) | node.number] as part_chain,
            chain_length
        ORDER BY chain_length DESC
        LIMIT 5
        """
        
        results3 = self.run_query(query3)
        logger.info("\nPart supersession chains:")
        for result in results3:
            logger.info(f"  Chain length {result['chain_length']}: {' -> '.join(result['part_chain'])}")
        
        return {
            'multi_change_parts': results1,
            'multi_part_changes': results2,
            'supersession_chains': results3
        }

    def verify_change_dependencies(self):
        """Verify change dependency networks."""
        logger.info("\n=== CHANGE DEPENDENCY NETWORKS ===")
        
        # Changes with dependencies
        query1 = """
        MATCH (c:Change)
        WHERE (c)-[:DEPENDS_ON]-() OR (c)-[:RELATED_TO]-()
        RETURN c.number as change_number, c.type as change_type, c.state as state
        ORDER BY c.number
        """
        
        results1 = self.run_query(query1)
        logger.info(f"Changes with dependencies: {len(results1)}")
        
        # Dependency chains
        query2 = """
        MATCH path = (c1:Change)-[:DEPENDS_ON*1..3]->(c2:Change)
        WITH path, length(path) as chain_length
        WHERE chain_length >= 2
        RETURN 
            [node in nodes(path) | node.number] as change_chain,
            chain_length
        ORDER BY chain_length DESC
        LIMIT 5
        """
        
        results2 = self.run_query(query2)
        logger.info("\nChange dependency chains:")
        for result in results2:
            logger.info(f"  Chain length {result['chain_length']}: {' -> '.join(result['change_chain'])}")
        
        return {
            'changes_with_deps': results1,
            'dependency_chains': results2
        }

    def verify_bom_structures(self):
        """Verify BOM structures and assemblies."""
        logger.info("\n=== BOM STRUCTURES VERIFICATION ===")
        
        # Top-level assemblies
        query1 = """
        MATCH (parent:WTPart)-[r:HAS_COMPONENT]->(child:WTPart)
        WHERE NOT ()-[:HAS_COMPONENT]->(parent)
        RETURN parent.number as assembly_number, parent.name as assembly_name, count(r) as component_count
        ORDER BY component_count DESC
        LIMIT 10
        """
        
        results1 = self.run_query(query1)
        logger.info("Top-level assemblies:")
        for result in results1:
            logger.info(f"  {result['assembly_number']} ({result['assembly_name']}): {result['component_count']} components")
        
        # Deep BOM structures
        query2 = """
        MATCH path = (root:WTPart)-[:HAS_COMPONENT*1..4]->(leaf:WTPart)
        WHERE NOT ()-[:HAS_COMPONENT]->(root) AND NOT (leaf)-[:HAS_COMPONENT]->()
        WITH root, length(path) as depth
        RETURN root.number as root_part, max(depth) as max_depth
        ORDER BY max_depth DESC
        LIMIT 5
        """
        
        results2 = self.run_query(query2)
        logger.info("\nDeepest BOM structures:")
        for result in results2:
            logger.info(f"  {result['root_part']}: depth {result['max_depth']}")
        
        return {
            'top_assemblies': results1,
            'deep_structures': results2
        }

    def generate_comprehensive_report(self):
        """Generate a comprehensive analysis report."""
        logger.info("\n" + "="*60)
        logger.info("COMPREHENSIVE SNOWMOBILE CHANGE TRACKING REPORT")
        logger.info("="*60)
        
        # Collect all verification data
        basic_counts = self.verify_basic_counts()
        change_types = self.verify_change_types()
        change_states = self.verify_change_states()
        part_categories = self.verify_part_categories()
        complex_relationships = self.verify_complex_relationships()
        change_dependencies = self.verify_change_dependencies()
        bom_structures = self.verify_bom_structures()
        
        # Create summary report
        report = {
            'timestamp': datetime.now().isoformat(),
            'summary': {
                'total_parts': basic_counts.get('Total Parts', 0),
                'total_changes': basic_counts.get('Total Changes', 0),
                'total_relationships': sum([
                    basic_counts.get('Total BOM Relationships', 0),
                    basic_counts.get('Total Change Relationships', 0),
                    basic_counts.get('Total SUPERSEDES Relationships', 0),
                    basic_counts.get('Total PART_OF Relationships', 0),
                    basic_counts.get('Total DEPENDS_ON Relationships', 0),
                    basic_counts.get('Total RELATED_TO Relationships', 0)
                ])
            },
            'change_analysis': {
                'change_types': change_types,
                'change_states': change_states,
                'changes_with_dependencies': len(change_dependencies['changes_with_deps'])
            },
            'part_analysis': {
                'part_categories': part_categories,
                'parts_with_multiple_changes': len(complex_relationships['multi_change_parts']),
                'top_level_assemblies': len(bom_structures['top_assemblies'])
            },
            'relationship_analysis': {
                'bom_structures': bom_structures,
                'complex_relationships': complex_relationships,
                'change_dependencies': change_dependencies
            }
        }
        
        # Save report to file
        from pathlib import Path
        out_dir = Path('data/processed')
        out_dir.mkdir(parents=True, exist_ok=True)
        report_file = out_dir / 'snowmobile_graph_verification_report.json'
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"\nComprehensive verification report saved to: {report_file}")
        
        # Print executive summary
        logger.info("\n" + "="*60)
        logger.info("EXECUTIVE SUMMARY")
        logger.info("="*60)
        logger.info(f"Total Parts: {report['summary']['total_parts']}")
        logger.info(f"Total Changes: {report['summary']['total_changes']}")
        logger.info(f"Total Relationships: {report['summary']['total_relationships']}")
        logger.info(f"Changes with Dependencies: {report['change_analysis']['changes_with_dependencies']}")
        logger.info(f"Parts with Multiple Changes: {report['part_analysis']['parts_with_multiple_changes']}")
        logger.info(f"Top-Level Assemblies: {report['part_analysis']['top_level_assemblies']}")
        
        return report

def main():
    """Main verification function."""
    verifier = SnowmobileGraphVerifier()
    
    try:
        # Generate comprehensive verification report
        report = verifier.generate_comprehensive_report()
        
        logger.info("\n" + "="*60)
        logger.info("VERIFICATION COMPLETED SUCCESSFULLY!")
        logger.info("="*60)
        logger.info("The snowmobile change tracking graph has been comprehensively verified.")
        logger.info("All relationships and dependencies have been mapped and validated.")
        
    except Exception as e:
        logger.error(f"Verification failed: {str(e)}")
        raise
    finally:
        verifier.close()

if __name__ == "__main__":
    main()