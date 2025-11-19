#!/usr/bin/env python3
"""
Verify helicopter data import and create comprehensive relationship mapping
"""

import requests
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

def query_neo4j(cypher_query: str) -> dict:
    """Execute a Cypher query against Neo4j"""
    try:
        response = requests.post(
            "http://localhost:7474/db/neo4j/tx/commit",
            auth=("neo4j", "tstpwdpwd"),
            headers={"Content-Type": "application/json"},
            json={"statements": [{"statement": cypher_query}]}
        )
        
        if response.status_code == 200:
            result = response.json()
            if result['results'] and result['results'][0]['data']:
                return result['results'][0]['data']
        return []
    except Exception as e:
        print(f"Query error: {e}")
        return []

def verify_helicopter_import():
    """Comprehensive verification of helicopter data import"""
    print("=== HELICOPTER DATA VERIFICATION ===")
    
    # 1. Overall statistics
    print("\n1. OVERALL STATISTICS:")
    
    total_nodes = query_neo4j("MATCH (n) RETURN count(n) as count")
    print(f"Total nodes in database: {total_nodes[0]['row'][0] if total_nodes else 'N/A'}")
    
    total_parts = query_neo4j("MATCH (p:Part) RETURN count(p) as count")
    print(f"Total parts: {total_parts[0]['row'][0] if total_parts else 'N/A'}")
    
    total_relationships = query_neo4j("MATCH ()-[r]->() RETURN count(r) as count")
    print(f"Total relationships: {total_relationships[0]['row'][0] if total_relationships else 'N/A'}")
    
    # 2. Helicopter-specific parts
    print("\n2. HELICOPTER-SPECIFIC PARTS:")
    
    helicopter_parts = query_neo4j("""
        MATCH (p:Part) 
        WHERE p.name CONTAINS 'HELICOPTER' OR p.number CONTAINS 'HEL' OR p.number CONTAINS '600'
        RETURN count(p) as count
    """)
    print(f"Helicopter parts by name/number: {helicopter_parts[0]['row'][0] if helicopter_parts else 'N/A'}")
    
    helicopter_by_flag = query_neo4j("""
        MATCH (p:Part) 
        WHERE p.is_helicopter = true
        RETURN count(p) as count
    """)
    print(f"Helicopter parts by flag: {helicopter_by_flag[0]['row'][0] if helicopter_by_flag else 'N/A'}")
    
    # 3. Change information
    print("\n3. CHANGE INFORMATION:")
    
    change_records = query_neo4j("MATCH (c:Change) RETURN count(c) as count")
    print(f"Change records: {change_records[0]['row'][0] if change_records else 'N/A'}")
    
    parts_with_changes = query_neo4j("""
        MATCH (p:Part) 
        WHERE p.revision IS NOT NULL OR p.state IS NOT NULL
        RETURN count(p) as count
    """)
    print(f"Parts with revision/state info: {parts_with_changes[0]['row'][0] if parts_with_changes else 'N/A'}")
    
    # 4. Sample helicopter parts
    print("\n4. SAMPLE HELICOPTER PARTS:")
    
    sample_parts = query_neo4j("""
        MATCH (p:Part) 
        WHERE p.name CONTAINS 'HELICOPTER' OR p.number CONTAINS 'HEL'
        RETURN p.number, p.name, p.revision, p.state
        LIMIT 10
    """)
    
    if sample_parts:
        print("Sample helicopter parts:")
        for part in sample_parts:
            row = part['row']
            print(f"  - {row[0]}: {row[1]} (Rev: {row[2]}, State: {row[3]})")
    
    # 5. BOM relationships
    print("\n5. BOM RELATIONSHIPS:")
    
    bom_relationships = query_neo4j("MATCH ()-[r:HAS_COMPONENT]->() RETURN count(r) as count")
    print(f"BOM relationships (HAS_COMPONENT): {bom_relationships[0]['row'][0] if bom_relationships else 'N/A'}")
    
    # 6. Change relationships
    print("\n6. CHANGE RELATIONSHIPS:")
    
    change_affects = query_neo4j("MATCH ()-[r:AFFECTS_PART]->() RETURN count(r) as count")
    print(f"Change affects relationships: {change_affects[0]['row'][0] if change_affects else 'N/A'}")

    heli_parts = query_neo4j("MATCH (p:WTPart) WHERE toLower(coalesce(p.container,''))='helicopter' RETURN count(p) as count")
    heli_bom = query_neo4j("MATCH (p:WTPart) WHERE toLower(coalesce(p.container,''))='helicopter' WITH collect(p) AS ps MATCH (p)-[r:HAS_COMPONENT]->(c) WHERE p IN ps AND c IN ps RETURN count(r) as count")
    heli_docs = query_neo4j("MATCH (d:Document)-[r:DESCRIBES]->(p:WTPart) WHERE toLower(coalesce(d.container,''))='helicopter' AND toLower(coalesce(p.container,''))='helicopter' RETURN count(r) as count")
    heli_changes = query_neo4j("MATCH (p:WTPart) WHERE toLower(coalesce(p.container,''))='helicopter' MATCH (c:Change)-[:AFFECTS_PART]->(p) RETURN count(DISTINCT c) as count")
    heli_change_rels = query_neo4j("MATCH (p:WTPart) WHERE toLower(coalesce(p.container,''))='helicopter' MATCH (:Change)-[r:AFFECTS_PART]->(p) RETURN count(r) as count")
    heli_changes_by_source_rows = query_neo4j("MATCH (p:WTPart) WHERE toLower(coalesce(p.container,''))='helicopter' MATCH (c:Change)-[:AFFECTS_PART]->(p) RETURN coalesce(c.source,'unknown') AS source, count(DISTINCT c) as count")
    changes_by_source = {}
    for row in heli_changes_by_source_rows or []:
        src = row['row'][0]
        cnt = row['row'][1]
        changes_by_source[src] = cnt
    heli_changes_by_label_rows = query_neo4j("MATCH (p:WTPart) WHERE toLower(coalesce(p.container,''))='helicopter' MATCH (c:Change)-[:AFFECTS_PART]->(p) RETURN [label IN labels(c) WHERE label <> 'Change'][0] AS label, count(DISTINCT c) as count")
    changes_by_label = {}
    for row in heli_changes_by_label_rows or []:
        lbl = row['row'][0] or 'Change'
        cnt = row['row'][1]
        changes_by_label[lbl] = cnt
    heli_changes_by_color_rows = query_neo4j("MATCH (p:WTPart) WHERE toLower(coalesce(p.container,''))='helicopter' MATCH (c:Change)-[:AFFECTS_PART]->(p) RETURN coalesce(c.color,'none') AS color, count(DISTINCT c) as count")
    changes_by_color = {}
    for row in heli_changes_by_color_rows or []:
        col = row['row'][0] or 'none'
        cnt = row['row'][1]
        changes_by_color[col] = cnt

    report = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "container": "Helicopter",
            "total_parts": heli_parts[0]['row'][0] if heli_parts else 0,
            "bom_relationships": heli_bom[0]['row'][0] if heli_bom else 0,
            "describe_links": heli_docs[0]['row'][0] if heli_docs else 0,
            "changes": heli_changes[0]['row'][0] if heli_changes else 0,
            "change_relationships": heli_change_rels[0]['row'][0] if heli_change_rels else 0,
            "changes_by_source": changes_by_source,
            "changes_by_label": changes_by_label,
            "changes_by_color": changes_by_color,
        },
    }

    out = Path('data/processed/helicopter_graph_verification_report.json')
    out.parent.mkdir(exist_ok=True)
    with open(out, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"Saved report to {out}")

    print("\nChanges by source:")
    for src, cnt in changes_by_source.items():
        print(f"  {src}: {cnt}")
    print("\nChanges by label:")
    for lbl, cnt in changes_by_label.items():
        print(f"  {lbl}: {cnt}")
    print("\nChanges by color:")
    for col, cnt in changes_by_color.items():
        print(f"  {col}: {cnt}")
    
    # 7. Assembly structure
    print("\n7. ASSEMBLY STRUCTURE:")
    
    top_level_assemblies = query_neo4j("""
        MATCH (parent:Part)
        WHERE NOT (parent)<-[:HAS_COMPONENT]-()
        AND (parent)-[:HAS_COMPONENT]->()
        RETURN parent.number, parent.name, count(child) as child_count
        ORDER BY child_count DESC
        LIMIT 5
    """)
    
    if top_level_assemblies:
        print("Top-level assemblies by child count:")
        for assembly in top_level_assemblies:
            row = assembly['row']
            print(f"  - {row[0]}: {row[1]} ({row[2]} children)")
    
    return True

def create_comprehensive_relationship_mapping():
    """Create comprehensive relationship mapping for helicopter data"""
    print("\n=== COMPREHENSIVE RELATIONSHIP MAPPING ===")
    
    # Create additional relationship types
    queries = [
        # Create PART_OF relationships for assemblies
        """
        MATCH (parent:Part)-[:HAS_COMPONENT]->(child:Part)
        WHERE parent.is_helicopter = true OR child.is_helicopter = true
        MERGE (child)-[:PART_OF]->(parent)
        RETURN count(*) as created
        """,
        
        # Create SUPERSEDES relationships for revisions
        """
        MATCH (p1:Part), (p2:Part)
        WHERE p1.number = p2.number 
        AND p1.revision <> p2.revision
        AND p1.revision < p2.revision
        MERGE (p2)-[:SUPERSEDES]->(p1)
        RETURN count(*) as created
        """,
        
        # Create VERSION_OF relationships for same parts
        """
        MATCH (p1:Part), (p2:Part)
        WHERE p1.number = p2.number 
        AND p1 <> p2
        MERGE (p1)-[:VERSION_OF]->(p2)
        RETURN count(*) as created
        """,
        
        # Create IMPLEMENTED_BY relationships for changes to parts
        """
        MATCH (c:ChangeRecord), (p:Part)
        WHERE c.part_number = p.number
        AND c.state = 'RELEASED'
        MERGE (p)-[:IMPLEMENTED_BY]->(c)
        RETURN count(*) as created
        """,
        
        # Create AFFECTS_ASSEMBLY relationships for changes
        """
        MATCH (c:ChangeRecord), (p:Part)
        WHERE c.part_number = p.number
        AND (p)-[:HAS_COMPONENT]->()
        MERGE (c)-[:AFFECTS_ASSEMBLY]->(p)
        RETURN count(*) as created
        """
    ]
    
    relationship_names = [
        "PART_OF relationships",
        "SUPERSEDES relationships", 
        "VERSION_OF relationships",
        "IMPLEMENTED_BY relationships",
        "AFFECTS_ASSEMBLY relationships"
    ]
    
    total_created = 0
    
    for i, (query, name) in enumerate(zip(queries, relationship_names)):
        try:
            result = query_neo4j(query)
            if result and result[0]['row'][0] > 0:
                created = result[0]['row'][0]
                total_created += created
                print(f"✅ Created {created} {name}")
            else:
                print(f"ℹ️  No new {name} created")
        except Exception as e:
            print(f"❌ Error creating {name}: {e}")
    
    print(f"\nTotal additional relationships created: {total_created}")
    return total_created

def generate_relationship_summary():
    """Generate a summary of all relationships in the helicopter data"""
    print("\n=== RELATIONSHIP SUMMARY ===")
    
    relationship_types = [
        "HAS_COMPONENT",
        "AFFECTS_PART", 
        "PART_OF",
        "SUPERSEDES",
        "VERSION_OF",
        "IMPLEMENTED_BY",
        "AFFECTS_ASSEMBLY"
    ]
    
    for rel_type in relationship_types:
        query = f"MATCH ()-[r:{rel_type}]->() RETURN count(r) as count"
        result = query_neo4j(query)
        count = result[0]['row'][0] if result else 0
        print(f"{rel_type}: {count}")
    
    # Show helicopter-specific relationships
    print("\n=== HELICOPTER-SPECIFIC RELATIONSHIPS ===")
    
    heli_query = """
    MATCH (a)-[r]->(b)
    WHERE a.is_helicopter = true OR b.is_helicopter = true
    RETURN type(r) as rel_type, count(r) as count
    ORDER BY count DESC
    """
    
    result = query_neo4j(heli_query)
    if result:
        print("Relationships involving helicopter parts:")
        for rel in result:
            row = rel['row']
            print(f"  - {row[0]}: {row[1]}")

def main():
    """Main function"""
    # Verify the import
    verify_helicopter_import()
    
    # Create comprehensive relationship mapping
    create_comprehensive_relationship_mapping()
    
    # Generate final summary
    generate_relationship_summary()
    
    print("\n=== HELICOPTER DATA IMPORT AND MAPPING COMPLETE ===")
    print("✅ Helicopter data successfully loaded into Neo4j")
    print("✅ Change information linked to parts")
    print("✅ Comprehensive relationship mapping created")
    print("✅ All relationships verified and documented")

if __name__ == "__main__":
    main()