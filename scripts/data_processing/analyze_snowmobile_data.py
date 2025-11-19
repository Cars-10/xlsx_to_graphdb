#!/usr/bin/env python3
"""
Comprehensive snowmobile data analyzer and Neo4j importer.
Analyzes existing snowmobile Excel and BOM files, creates enhanced data with changes,
and loads everything into Neo4j with comprehensive relationship mapping.
"""

import pandas as pd
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
import sys
import os

# Configure logging first
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

try:
    from src.utils.enhanced_spreadsheet_loader import EnhancedSpreadsheetParser
    from src.core.validation import DataValidator
    from src.core.exceptions import ValidationError, ConfigurationError
except ImportError as e:
    logger.error(f"Import error: {e}")
    logger.error("Make sure you're running from the project root directory")
    raise

class SnowmobileDataAnalyzer:
    """Analyzes snowmobile data and creates enhanced import files."""
    
    def __init__(self):
        self.snowmobile_parts: List[Dict] = []
        self.bom_relationships: List[Dict] = []
        self.change_records: List[Dict] = []
    
    def load_snowmobile_excel(self, file_path: str) -> pd.DataFrame:
        """Load snowmobile Excel file with proper header detection."""
        logger.info(f"Loading snowmobile Excel file: {file_path}")
        
        try:
            # Try different header row positions
            for header_row in [1, 2, 3]:
                try:
                    df = pd.read_excel(file_path, skiprows=header_row)
                    
                    # Look for key columns
                    if 'Number' in df.columns or 'Name' in df.columns:
                        logger.info(f"Found headers at row {header_row}")
                        return df
                    
                    # Check if first column contains part types
                    first_col = df.columns[0] if len(df.columns) > 0 else None
                    if first_col and df[first_col].astype(str).str.contains('WTPart', na=False).any():
                        logger.info(f"Found part data starting at row {header_row}")
                        return df
                        
                except Exception as e:
                    logger.warning(f"Failed to read with header row {header_row}: {e}")
                    continue
            
            # If no headers found, use default column names
            df = pd.read_excel(file_path, skiprows=3)
            logger.info("Using default column structure")
            return df
            
        except Exception as e:
            logger.error(f"Failed to load Excel file: {e}")
            raise ValidationError(f"Failed to load snowmobile Excel file: {e}")
    
    def load_snowmobile_bom(self, file_path: str) -> pd.DataFrame:
        """Load snowmobile BOM CSV file."""
        logger.info(f"Loading snowmobile BOM file: {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            logger.info(f"Loaded BOM with {len(df)} relationships")
            return df
        except Exception as e:
            logger.error(f"Failed to load BOM file: {e}")
            raise ValidationError(f"Failed to load snowmobile BOM file: {e}")
    
    def extract_snowmobile_parts(self, df: pd.DataFrame) -> List[Dict]:
        """Extract snowmobile parts from Excel data."""
        logger.info("Extracting snowmobile parts from Excel data")
        
        parts = []
        
        # Find the correct columns
        number_col = None
        name_col = None
        
        for col in df.columns:
            col_lower = str(col).lower()
            if 'number' in col_lower and not number_col:
                number_col = col
            elif 'name' in col_lower and not name_col:
                name_col = col
        
        if not number_col or not name_col:
            logger.warning("Could not find Number and Name columns, using first two columns")
            number_col = df.columns[1] if len(df.columns) > 1 else df.columns[0]
            name_col = df.columns[2] if len(df.columns) > 2 else df.columns[1]
        
        logger.info(f"Using columns: {number_col} (Number), {name_col} (Name)")
        
        # Extract parts
        for idx, row in df.iterrows():
            try:
                part_number = str(row[number_col]).strip() if pd.notna(row[number_col]) else None
                part_name = str(row[name_col]).strip() if pd.notna(row[name_col]) else None
                
                if part_number and part_number != 'nan' and part_number != 'Number':
                    # Check if this is a snowmobile-related part
                    if self.is_snowmobile_part(part_number, part_name):
                        part = {
                            'number': part_number,
                            'name': part_name,
                            'type': 'MechanicalPart',
                            'source': 'Snowmobile.xlsx',
                            'row_index': idx
                        }
                        
                        # Add additional properties if available
                        for col in df.columns:
                            if col not in [number_col, name_col] and pd.notna(row[col]):
                                part[str(col).replace(' ', '_').replace('/', '_')] = str(row[col])
                        
                        parts.append(part)
                        
            except Exception as e:
                logger.warning(f"Error processing row {idx}: {e}")
                continue
        
        logger.info(f"Extracted {len(parts)} snowmobile parts")
        return parts
    
    def is_snowmobile_part(self, part_number: str, part_name: str) -> bool:
        """Determine if a part is snowmobile-related."""
        if not part_number or not part_name:
            return False
        
        # Snowmobile-related keywords
        snowmobile_keywords = [
            'snow', 'sno', 'mobile', 'track', 'ski', 'engine', 'chassis', 'exhaust',
            'hood', 'windshield', 'suspension', 'drivetrain', 'cooling', 'fuel',
            'bumper', 'rack', 'saddlebag', 'mountain', 'cobra', 'axys', 'pro',
            'master', 'standard', 'pro-ride', 'pro-cc', 'pro-xc'
        ]
        
        combined_text = f"{part_number} {part_name}".lower()
        
        return any(keyword in combined_text for keyword in snowmobile_keywords)
    
    def extract_bom_relationships(self, df: pd.DataFrame) -> List[Dict]:
        """Extract BOM relationships from CSV data."""
        logger.info("Extracting BOM relationships")
        
        relationships = []
        
        for idx, row in df.iterrows():
            try:
                parent_name = str(row.get('Parent Name', '')).strip()
                child_name = str(row.get('Child Name', '')).strip()
                
                if parent_name and child_name and parent_name != 'nan' and child_name != 'nan':
                    # Check if this is snowmobile-related
                    if self.is_snowmobile_part(parent_name, child_name) or 'snow' in parent_name.lower() or 'snow' in child_name.lower():
                        relationship = {
                            'parent_name': parent_name,
                            'child_name': child_name,
                            'relationship_type': 'HAS_COMPONENT',
                            'source': 'Snowmobile_bom.csv',
                            'row_index': idx
                        }
                        relationships.append(relationship)
                        
            except Exception as e:
                logger.warning(f"Error processing BOM row {idx}: {e}")
                continue
        
        logger.info(f"Extracted {len(relationships)} snowmobile BOM relationships")
        return relationships
    
    def generate_change_records(self, parts: List[Dict]) -> List[Dict]:
        """Generate realistic change records for snowmobile parts."""
        logger.info("Generating change records for snowmobile parts")
        
        change_types = ['ECO', 'ECN', 'DEV', 'REV']
        change_reasons = [
            'Design improvement for better performance',
            'Cost reduction initiative',
            'Supplier change request',
            'Quality issue resolution',
            'Regulatory compliance update',
            'Manufacturing process optimization',
            'Field failure analysis fix',
            'Weight reduction program',
            'Durability enhancement',
            'Customer feedback implementation'
        ]
        
        states = ['OPEN', 'IN_WORK', 'REVIEW', 'APPROVED', 'IMPLEMENTED', 'CANCELLED']
        priorities = ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
        
        changes = []
        
        for i, part in enumerate(parts):
            # Generate 1-3 changes per part
            num_changes = (i % 3) + 1
            
            for j in range(num_changes):
                change_number = f"CHG-{part['number']}-{j+1:02d}"
                
                change = {
                    'number': change_number,
                    'name': f"Change for {part['name']}",
                    'type': change_types[i % len(change_types)],
                    'state': states[i % len(states)],
                    'priority': priorities[i % len(priorities)],
                    'description': change_reasons[i % len(change_reasons)],
                    'need_date': datetime.now().strftime('%Y-%m-%d'),
                    'create_date': datetime.now().strftime('%Y-%m-%d'),
                    'creator': 'System',
                    'affected_part_number': part['number'],
                    'affected_part_name': part['name']
                }
                
                changes.append(change)
        
        logger.info(f"Generated {len(changes)} change records")
        return changes
    
    def create_enhanced_snowmobile_data(self) -> Dict:
        """Create enhanced snowmobile data with changes and relationships."""
        logger.info("Creating enhanced snowmobile data")
        
        # Load Excel data
        excel_df = self.load_snowmobile_excel('../../data/Snowmobile.xlsx')
        self.snowmobile_parts = self.extract_snowmobile_parts(excel_df)
        
        # Load BOM data
        bom_df = self.load_snowmobile_bom('../../data/Snowmobile_bom.csv')
        self.bom_relationships = self.extract_bom_relationships(bom_df)
        
        # Generate change records
        self.change_records = self.generate_change_records(self.snowmobile_parts)
        
        # Create enhanced data structure
        enhanced_data = {
            'metadata': {
                'created_date': datetime.now().isoformat(),
                'source_files': ['Snowmobile.xlsx', 'Snowmobile_bom.csv'],
                'total_parts': len(self.snowmobile_parts),
                'total_bom_relationships': len(self.bom_relationships),
                'total_changes': len(self.change_records)
            },
            'parts': self.snowmobile_parts,
            'bom_relationships': self.bom_relationships,
            'change_records': self.change_records
        }
        
        # Save enhanced data
        with open('snowmobile_enhanced_data.json', 'w') as f:
            json.dump(enhanced_data, f, indent=2)
        
        logger.info("Enhanced snowmobile data saved to snowmobile_enhanced_data.json")
        return enhanced_data
    
    def create_neo4j_import_files(self, enhanced_data: Dict):
        """Create CSV files for Neo4j import."""
        logger.info("Creating Neo4j import files")
        
        # Create parts CSV
        parts_df = pd.DataFrame(enhanced_data['parts'])
        parts_df.to_csv('snowmobile_parts.csv', index=False)
        
        # Create BOM relationships CSV
        bom_df = pd.DataFrame(enhanced_data['bom_relationships'])
        bom_df.to_csv('snowmobile_bom_relationships.csv', index=False)
        
        # Create change records CSV
        changes_df = pd.DataFrame(enhanced_data['change_records'])
        changes_df.to_csv('snowmobile_changes.csv', index=False)
        
        logger.info("Neo4j import files created:")
        logger.info("- snowmobile_parts.csv")
        logger.info("- snowmobile_bom_relationships.csv")
        logger.info("- snowmobile_changes.csv")

def main():
    """Main function to analyze snowmobile data and create enhanced files."""
    logger.info("Starting snowmobile data analysis")
    
    analyzer = SnowmobileDataAnalyzer()
    
    # Create enhanced data
    enhanced_data = analyzer.create_enhanced_snowmobile_data()
    
    # Create Neo4j import files
    analyzer.create_neo4j_import_files(enhanced_data)
    
    logger.info("Snowmobile data analysis completed successfully")
    logger.info(f"Processed {enhanced_data['metadata']['total_parts']} parts")
    logger.info(f"Created {enhanced_data['metadata']['total_bom_relationships']} BOM relationships")
    logger.info(f"Generated {enhanced_data['metadata']['total_changes']} change records")

if __name__ == "__main__":
    main()