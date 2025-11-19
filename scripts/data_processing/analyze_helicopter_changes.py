#!/usr/bin/env python3
"""
Enhanced Helicopter data analysis with change detection
"""

import pandas as pd
import json
from pathlib import Path
import re

def analyze_helicopter_parts():
    """Analyze helicopter-specific parts and identify changes"""
    data_dir = Path("/Users/cars10/GIT/KTB3/windchill_demo_data/data")
    
    # Read the main helicopter data
    excel_path = data_dir / "Helicopter.xlsx"
    bom_path = data_dir / "Helicopter_bom.csv"
    
    helicopter_parts = []
    change_info = []
    
    if excel_path.exists():
        try:
            # Read different sheets to find helicopter parts
            excel_file = pd.ExcelFile(excel_path)
            
            for sheet_name in excel_file.sheet_names:
                if sheet_name in ['MechanicalPart-Sheet', 'Helicopter-Sheet', 'WTPart-Sheet']:
                    df = pd.read_excel(excel_path, sheet_name=sheet_name)
                    
                    # Skip header rows that are empty
                    df_clean = df.dropna(how='all')
                    if len(df_clean) > 3:
                        # Use the 4th row as headers (based on analysis)
                        headers = df_clean.iloc[0].tolist()
                        df_data = df_clean.iloc[1:].copy()
                        df_data.columns = headers
                        
                        print(f"\n=== Analyzing {sheet_name} ===")
                        print(f"Columns: {headers}")
                        
                        # Look for helicopter-related parts
                        if 'Name' in df_data.columns:
                            helicopter_names = df_data[df_data['Name'].str.contains('helicopter|Helicopter|HELI', na=False, case=False)]
                            if not helicopter_names.empty:
                                print(f"Found {len(helicopter_names)} helicopter parts by name")
                                helicopter_parts.extend(helicopter_names.to_dict('records'))
                        
                        if 'Number' in df_data.columns:
                            # Look for helicopter part numbers (often contain specific patterns)
                            helicopter_numbers = df_data[df_data['Number'].str.contains('HEL|HELI|600', na=False, case=False)]
                            if not helicopter_numbers.empty:
                                print(f"Found {len(helicopter_numbers)} helicopter parts by number")
                                helicopter_parts.extend(helicopter_numbers.to_dict('records'))
                        
                        # Look for change information
                        change_keywords = ['Change', 'Revision', 'Version', 'Date', 'State', 'Effectivity']
                        change_columns = [col for col in df_data.columns if any(keyword in str(col) for keyword in change_keywords)]
                        
                        if change_columns:
                            print(f"Found change columns: {change_columns}")
                            change_data = df_data[change_columns].dropna(how='all')
                            if not change_data.empty:
                                change_info.extend(change_data.to_dict('records'))
                                
        except Exception as e:
            print(f"Error analyzing Excel file: {e}")
    
    # Analyze BOM relationships
    if bom_path.exists():
        try:
            bom_df = pd.read_csv(bom_path)
            print(f"\n=== BOM Analysis ===")
            print(f"Total relationships: {len(bom_df)}")
            
            # Look for helicopter-specific part numbers in BOM
            helicopter_parents = bom_df[bom_df['Parent Name'].str.contains('HEL|HELI|600', na=False, case=False)]
            helicopter_children = bom_df[bom_df['Child Name'].str.contains('HEL|HELI|600', na=False, case=False)]
            
            print(f"Helicopter parent relationships: {len(helicopter_parents)}")
            print(f"Helicopter child relationships: {len(helicopter_children)}")
            
            if not helicopter_parents.empty:
                print("Helicopter parent parts:")
                print(helicopter_parents['Parent Name'].unique())
            
            if not helicopter_children.empty:
                print("Helicopter child parts:")
                print(helicopter_children['Child Name'].unique())
                
        except Exception as e:
            print(f"Error analyzing BOM file: {e}")
    
    return {
        'helicopter_parts': helicopter_parts,
        'change_info': change_info,
        'analysis_summary': {
            'total_helicopter_parts': len(helicopter_parts),
            'total_change_records': len(change_info)
        }
    }

def create_change_data_model():
    """Create a data model for changes that can be loaded into Neo4j"""
    return {
        'change_types': [
            'Engineering Change Notice (ECN)',
            'Engineering Change Order (ECO)', 
            'Deviation',
            'Waiver',
            'Stop Ship',
            'Part Revision',
            'Document Revision'
        ],
        'change_states': [
            'Open',
            'In Review', 
            'Approved',
            'Implemented',
            'Rejected',
            'Cancelled'
        ],
        'relationships': [
            'AFFECTS_PART',
            'AFFECTS_DOCUMENT', 
            'REPLACES_PART',
            'SUPERSEDES_REVISION',
            'IMPLEMENTS_CHANGE',
            'REQUIRES_APPROVAL',
            'HAS_EFFECTIVITY'
        ]
    }

if __name__ == "__main__":
    print("Analyzing Helicopter data for changes and helicopter-specific parts...")
    
    results = analyze_helicopter_parts()
    change_model = create_change_data_model()
    
    # Save results
    data_dir = Path("/Users/cars10/GIT/KTB3/windchill_demo_data/data")
    with open(data_dir / "helicopter_change_analysis.json", "w") as f:
        json.dump({
            'helicopter_parts': results['helicopter_parts'],
            'change_info': results['change_info'],
            'analysis_summary': results['analysis_summary'],
            'change_model': change_model
        }, f, indent=2, default=str)
    
    print(f"\nAnalysis Summary:")
    print(f"Helicopter parts found: {results['analysis_summary']['total_helicopter_parts']}")
    print(f"Change records found: {results['analysis_summary']['total_change_records']}")
    print("Results saved to helicopter_change_analysis.json")