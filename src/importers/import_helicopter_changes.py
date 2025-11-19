#!/usr/bin/env python3
"""
Simplified Helicopter Change Importer - Uses existing neo4j_importer with enhanced data
"""

import pandas as pd
import json
import subprocess
import sys
import os
from pathlib import Path

def create_helicopter_import_files():
    """Create properly formatted files for Neo4j import"""
    data_dir = Path("/Users/cars10/GIT/KTB3/windchill_demo_data/data")
    
    # Read the enhanced helicopter data
    with open(data_dir / "helicopter_enhanced_data.json", "r") as f:
        enhanced_data = json.load(f)
    
    # Create helicopter parts Excel file (similar to Snowmobile.xlsx format)
    helicopter_parts = enhanced_data['parts']
    helicopter_df = pd.DataFrame(helicopter_parts)
    
    # Select relevant columns for import
    import_columns = ['Type', 'Number', 'Name', 'End Item', 'Phantom', 'Trace Code', 
                     'Generic Type', 'Service Kit', 'Serviceable', 'Assembly Mode', 
                     'Location', 'Organization ID', 'Revision', 'View', 'State', 
                     'Lifecycle', 'Source', 'Default Unit', 'Gathering Part', 
                     'Collapsible', 'Belt Length', 'Part Cost', 'Part Classification', 
                     'Material']
    
    # Filter available columns
    available_columns = [col for col in import_columns if col in helicopter_df.columns]
    export_df = helicopter_df[available_columns]
    
    # Create Excel file with multiple sheets (similar to original format)
    with pd.ExcelWriter(data_dir / "Helicopter_Import.xlsx", engine='openpyxl') as writer:
        # Main parts sheet
        export_df.to_excel(writer, sheet_name='HelicopterPart-Sheet', index=False)
        
        # Change information sheet
        if enhanced_data['changes']:
            changes_df = pd.DataFrame(enhanced_data['changes'])
            changes_df.to_excel(writer, sheet_name='ChangeInfo-Sheet', index=False)
    
    print(f"Created Helicopter_Import.xlsx with {len(export_df)} helicopter parts")
    
    # Create enhanced BOM with change relationships
    original_bom = pd.read_csv(data_dir / "Helicopter_bom.csv")
    
    # Add change information to BOM relationships
    enhanced_bom = original_bom.copy()
    enhanced_bom['_has_changes'] = False
    enhanced_bom['_change_revision'] = ''
    enhanced_bom['_change_state'] = ''
    
    # Link changes to BOM relationships based on part numbers
    for change in enhanced_data['changes']:
        part_num = change.get('_part_number', '')
        if part_num:
            # Mark relationships involving this part
            mask = (enhanced_bom['Parent Name'] == part_num) | (enhanced_bom['Child Name'] == part_num)
            enhanced_bom.loc[mask, '_has_changes'] = True
            enhanced_bom.loc[mask, '_change_revision'] = str(change.get('Revision', ''))
            enhanced_bom.loc[mask, '_change_state'] = str(change.get('State', ''))
    
    # Save enhanced BOM
    enhanced_bom.to_csv(data_dir / "Helicopter_bom_enhanced.csv", index=False)
    print(f"Created enhanced BOM with {len(enhanced_bom)} relationships, {enhanced_bom['_has_changes'].sum()} with changes")
    
    return {
        'parts_file': str(data_dir / "Helicopter_Import.xlsx"),
        'bom_file': str(data_dir / "Helicopter_bom_enhanced.csv"),
        'total_parts': len(export_df),
        'total_changes': len(enhanced_data['changes']),
        'relationships_with_changes': enhanced_bom['_has_changes'].sum()
    }

def import_helicopter_to_neo4j():
    """Import helicopter data to Neo4j using existing importer"""
    data_dir = Path("/Users/cars10/GIT/KTB3/windchill_demo_data/data")
    
    # Create import files
    files_info = create_helicopter_import_files()
    
    print("\n=== IMPORTING HELICOPTER DATA TO NEO4J ===")
    
    # Use the existing neo4j_importer.py with helicopter data
    cmd = [
        sys.executable, "neo4j_importer.py",
        "--excel", files_info['parts_file'],
        "--bom", files_info['bom_file'],
        "--uri", "bolt://localhost:7687",
        "--user", "neo4j"
    ]
    
    # Set environment variable for password
    env = os.environ.copy()
    env['NEO4J_PASSWORD'] = 'tstpwdpwd'
    
    print(f"Running command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True, cwd="/Users/cars10/GIT/KTB3/windchill_demo_data")
        
        print("=== IMPORT OUTPUT ===")
        print("STDOUT:")
        print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        
        if result.returncode == 0:
            print("\n✅ Helicopter data imported successfully!")
            print(f"Parts imported: {files_info['total_parts']}")
            print(f"Changes tracked: {files_info['total_changes']}")
            print(f"Relationships with changes: {files_info['relationships_with_changes']}")
            
            # Verify import by querying Neo4j
            verify_helicopter_import()
            
            return True
        else:
            print(f"\n❌ Import failed with return code: {result.returncode}")
            return False
            
    except Exception as e:
        print(f"Error running import: {e}")
        return False

def verify_helicopter_import():
    """Verify the helicopter data was imported correctly"""
    import requests
    import json
    
    print("\n=== VERIFYING HELICOPTER IMPORT ===")
    
    # Query Neo4j to verify data
    queries = [
        ("Total helicopter parts", "MATCH (p:Part) WHERE p.name CONTAINS 'HELICOPTER' OR p.number CONTAINS 'HEL' RETURN count(p) as count"),
        ("Parts with changes", "MATCH (p:Part) WHERE p.revision IS NOT NULL OR p.state IS NOT NULL RETURN count(p) as count"),
        ("BOM relationships", "MATCH ()-[r:HAS_COMPONENT]->() RETURN count(r) as count"),
        ("Sample helicopter parts", "MATCH (p:Part) WHERE p.name CONTAINS 'HELICOPTER' RETURN p.number, p.name, p.revision LIMIT 5")
    ]
    
    for query_name, cypher_query in queries:
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
                    data = result['results'][0]['data']
                    print(f"{query_name}: {data[0]['row'][0] if data else 'No results'}")
                    
                    if query_name == "Sample helicopter parts" and data:
                        print("Sample parts:")
                        for row in data:
                            print(f"  - {row['row'][0]}: {row['row'][1]} (Rev: {row['row'][2]})")
            else:
                print(f"{query_name}: Query failed")
                
        except Exception as e:
            print(f"{query_name}: Error - {e}")

if __name__ == "__main__":
    success = import_helicopter_to_neo4j()
    sys.exit(0 if success else 1)