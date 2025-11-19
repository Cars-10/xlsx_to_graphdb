#!/usr/bin/env python3
"""
Simple helicopter data loader using existing neo4j_importer with change tracking
"""

import pandas as pd
import json
import logging
from pathlib import Path
import subprocess
import sys

def analyze_helicopter_data_detailed():
    """Detailed analysis of helicopter data structure"""
    data_dir = Path("/Users/cars10/GIT/KTB3/windchill_demo_data/data")
    
    # Read Excel file and understand structure
    excel_path = data_dir / "Helicopter.xlsx"
    
    print("=== HELICOPTER DATA ANALYSIS ===")
    
    try:
        excel_file = pd.ExcelFile(excel_path)
        print(f"Available sheets: {excel_file.sheet_names}")
        
        # Process each sheet to find actual data
        for sheet_name in ['MechanicalPart-Sheet', 'WTPart-Sheet']:
            if sheet_name in excel_file.sheet_names:
                print(f"\n--- {sheet_name} ---")
                df = pd.read_excel(excel_path, sheet_name=sheet_name)
                
                print(f"Total rows: {len(df)}")
                print(f"Columns: {list(df.columns)}")
                
                # Look for the actual header row
                for i in range(min(10, len(df))):
                    row_data = df.iloc[i].tolist()
                    if 'Number' in str(row_data) and 'Name' in str(row_data):
                        print(f"Header row found at index {i}")
                        print(f"Row {i} data: {row_data}")
                        
                        # Extract data from this point
                        headers = row_data
                        data_start = i + 1
                        
                        if data_start < len(df):
                            df_data = df.iloc[data_start:].copy()
                            df_data.columns = headers
                            
                            print(f"Data rows: {len(df_data)}")
                            
                            # Look for helicopter parts
                            helicopter_parts = []
                            if 'Name' in df_data.columns:
                                helicopter_parts.extend(df_data[df_data['Name'].str.contains('helicopter|Helicopter|HELI', na=False, case=False)].to_dict('records'))
                            
                            if 'Number' in df_data.columns:
                                helicopter_parts.extend(df_data[df_data['Number'].str.contains('HEL|HELI|600', na=False, case=False)].to_dict('records'))
                            
                            # Remove duplicates
                            seen = set()
                            unique_parts = []
                            for part in helicopter_parts:
                                part_key = str(part.get('Number', '')) + str(part.get('Name', ''))
                                if part_key not in seen:
                                    seen.add(part_key)
                                    unique_parts.append(part)
                            
                            print(f"Helicopter parts found: {len(unique_parts)}")
                            if unique_parts:
                                print("Sample helicopter parts:")
                                for part in unique_parts[:3]:
                                    print(f"  - {part.get('Number', 'N/A')}: {part.get('Name', 'N/A')}")
                            
                            # Look for change information
                            change_columns = [col for col in df_data.columns if any(keyword in str(col) for keyword in ['Revision', 'State', 'Version'])]
                            print(f"Change columns: {change_columns}")
                            
                            if change_columns:
                                change_data = df_data[change_columns].dropna(how='all')
                                print(f"Change records: {len(change_data)}")
                        
                        break
                
    except Exception as e:
        print(f"Error: {e}")
    
    # Analyze BOM file
    bom_path = data_dir / "Helicopter_bom.csv"
    if bom_path.exists():
        print(f"\n--- BOM File ---")
        df_bom = pd.read_csv(bom_path)
        print(f"BOM relationships: {len(df_bom)}")
        print(f"Unique parents: {df_bom['Parent Name'].nunique()}")
        print(f"Unique children: {df_bom['Child Name'].nunique()}")
        
        # Look for helicopter parts in BOM
        heli_parents = df_bom[df_bom['Parent Name'].str.contains('HEL|HELI|600', na=False, case=False)]
        heli_children = df_bom[df_bom['Child Name'].str.contains('HEL|HELI|600', na=False, case=False)]
        
        print(f"Helicopter parent relationships: {len(heli_parents)}")
        print(f"Helicopter child relationships: {len(heli_children)}")

def create_enhanced_helicopter_files():
    """Create enhanced files with change information"""
    data_dir = Path("/Users/cars10/GIT/KTB3/windchill_demo_data/data")
    
    # Read and process helicopter data
    excel_path = data_dir / "Helicopter.xlsx"
    
    try:
        excel_file = pd.ExcelFile(excel_path)
        all_parts = []
        all_changes = []
        
        # Process MechanicalPart-Sheet
        if 'MechanicalPart-Sheet' in excel_file.sheet_names:
            df = pd.read_excel(excel_path, sheet_name='MechanicalPart-Sheet')
            
            # Find header row
            header_idx = None
            for i in range(min(10, len(df))):
                if 'Number' in str(df.iloc[i].tolist()) and 'Name' in str(df.iloc[i].tolist()):
                    header_idx = i
                    break
            
            if header_idx is not None:
                headers = df.iloc[header_idx].tolist()
                df_data = df.iloc[header_idx + 1:].copy()
                df_data.columns = headers
                
                # Add all parts (mark as helicopter if they match criteria)
                for _, row in df_data.iterrows():
                    part_data = row.to_dict()
                    part_data['_is_helicopter'] = False
                    
                    # Check if it's a helicopter part
                    if pd.notna(part_data.get('Name', '')) and 'helicopter' in str(part_data['Name']).lower():
                        part_data['_is_helicopter'] = True
                    if pd.notna(part_data.get('Number', '')) and any(pattern in str(part_data['Number']) for pattern in ['HEL', 'HELI', '600']):
                        part_data['_is_helicopter'] = True
                    
                    all_parts.append(part_data)
                
                # Extract change information
                change_cols = [col for col in df_data.columns if any(keyword in str(col) for keyword in ['Revision', 'State'])]
                if change_cols:
                    for _, row in df_data.iterrows():
                        change_data = {col: row[col] for col in change_cols if pd.notna(row[col])}
                        if change_data:
                            change_data['_part_number'] = row.get('Number', '')
                            change_data['_part_name'] = row.get('Name', '')
                            all_changes.append(change_data)
        
        print(f"Total parts processed: {len(all_parts)}")
        print(f"Helicopter parts: {sum(1 for p in all_parts if p.get('_is_helicopter', False))}")
        print(f"Change records: {len(all_changes)}")
        
        # Save enhanced data
        enhanced_data = {
            'parts': all_parts,
            'changes': all_changes,
            'metadata': {
                'total_parts': len(all_parts),
                'helicopter_parts': sum(1 for p in all_parts if p.get('_is_helicopter', False)),
                'change_records': len(all_changes),
                'processed_at': str(pd.Timestamp.now())
            }
        }
        
        with open(data_dir / "helicopter_enhanced_data.json", "w") as f:
            json.dump(enhanced_data, f, indent=2, default=str)
        
        print("Enhanced helicopter data saved to helicopter_enhanced_data.json")
        
        # Create a simple CSV for Neo4j import
        helicopter_parts_df = pd.DataFrame([p for p in all_parts if p.get('_is_helicopter', False)])
        if not helicopter_parts_df.empty:
            helicopter_parts_df.to_csv(data_dir / "helicopter_parts.csv", index=False)
            print(f"Helicopter parts CSV saved with {len(helicopter_parts_df)} parts")
        
        # Create changes CSV
        if all_changes:
            changes_df = pd.DataFrame(all_changes)
            changes_df.to_csv(data_dir / "helicopter_changes.csv", index=False)
            print(f"Helicopter changes CSV saved with {len(all_changes)} changes")
            
    except Exception as e:
        print(f"Error creating enhanced files: {e}")

if __name__ == "__main__":
    print("=== HELICOPTER DATA ANALYSIS ===")
    analyze_helicopter_data_detailed()
    
    print("\n=== CREATING ENHANCED FILES ===")
    create_enhanced_helicopter_files()