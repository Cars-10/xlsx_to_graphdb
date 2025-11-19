#!/usr/bin/env python3
"""
Analyze Helicopter data files to identify changes and structure
"""

import pandas as pd
import json
from pathlib import Path

def analyze_excel_file(file_path):
    """Analyze Excel file structure and content"""
    try:
        # Read all sheets
        excel_file = pd.ExcelFile(file_path)
        print(f"Excel file: {file_path}")
        print(f"Available sheets: {excel_file.sheet_names}")
        
        results = {}
        
        for sheet_name in excel_file.sheet_names:
            print(f"\n=== Sheet: {sheet_name} ===")
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
            print(f"Columns: {list(df.columns)}")
            
            # Show first few rows
            print(f"First 5 rows:")
            print(df.head())
            
            results[sheet_name] = {
                'rows': len(df),
                'columns': list(df.columns),
                'data_sample': df.head(10).to_dict('records')
            }
            
            # Look for helicopter-specific data
            if any('helicopter' in str(col).lower() for col in df.columns):
                print("Found helicopter-related columns!")
            
            # Check for change-related columns
            change_keywords = ['change', 'revision', 'version', 'effectivity', 'date', 'state']
            change_columns = [col for col in df.columns if any(keyword in str(col).lower() for keyword in change_keywords)]
            if change_columns:
                print(f"Change-related columns: {change_columns}")
                results[sheet_name]['change_columns'] = change_columns
        
        return results
        
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return None

def analyze_bom_file(file_path):
    """Analyze BOM CSV file"""
    try:
        df = pd.read_csv(file_path)
        print(f"\n=== BOM File: {file_path} ===")
        print(f"Rows: {len(df)}, Columns: {len(df.columns)}")
        print(f"Columns: {list(df.columns)}")
        print(f"First 10 rows:")
        print(df.head(10))
        
        # Analyze parent-child relationships
        unique_parents = df['Parent Name'].unique()
        unique_children = df['Child Name'].unique()
        
        print(f"\nUnique parents: {len(unique_parents)}")
        print(f"Unique children: {len(unique_children)}")
        print(f"Top 10 parents by child count:")
        parent_counts = df['Parent Name'].value_counts().head(10)
        print(parent_counts)
        
        return {
            'total_relationships': len(df),
            'unique_parents': len(unique_parents),
            'unique_children': len(unique_children),
            'top_parents': parent_counts.to_dict()
        }
        
    except Exception as e:
        print(f"Error reading BOM file: {e}")
        return None

if __name__ == "__main__":
    data_dir = Path("/Users/cars10/GIT/KTB3/windchill_demo_data/data")
    
    # Analyze Excel file
    excel_path = data_dir / "Helicopter.xlsx"
    if excel_path.exists():
        excel_results = analyze_excel_file(excel_path)
        
        # Save analysis results
        if excel_results:
            with open(data_dir / "helicopter_excel_analysis.json", "w") as f:
                json.dump(excel_results, f, indent=2, default=str)
    
    # Analyze BOM file
    bom_path = data_dir / "Helicopter_bom.csv"
    if bom_path.exists():
        bom_results = analyze_bom_file(bom_path)
        
        # Save analysis results
        if bom_results:
            with open(data_dir / "helicopter_bom_analysis.json", "w") as f:
                json.dump(bom_results, f, indent=2, default=str)
    
    print("\nAnalysis complete!")