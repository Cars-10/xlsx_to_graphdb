#!/usr/bin/env python3
"""
Convert hierarchical BOM (with Level column) to parent-child BOM CSV.
"""

import pandas as pd
import sys

def convert_hierarchical_to_parent_child(input_csv, output_csv):
    """Convert hierarchical BOM to parent-child relationship CSV."""

    # Read BOM, skip duplicate header
    df = pd.read_csv(input_csv, skiprows=1)

    # Filter to valid rows with Number
    df = df[df['Number'].notna()].copy()
    df['Number'] = df['Number'].astype(str).str.strip()
    df['Level'] = df['Level'].astype(int)

    # Build parent-child relationships based on hierarchy
    edges = []
    level_stack = {}  # Maps level to most recent part at that level

    for _, row in df.iterrows():
        part_number = row['Number']
        level = row['Level']

        # Track this part at its level
        level_stack[level] = part_number

        # If not root (level 0), find parent (most recent part at level-1)
        if level > 0:
            parent_level = level - 1
            if parent_level in level_stack:
                parent = level_stack[parent_level]
                edges.append({
                    'Parent Number': parent,
                    'Child Number': part_number
                })

        # Clear deeper levels
        levels_to_clear = [l for l in level_stack.keys() if l > level]
        for l in levels_to_clear:
            del level_stack[l]

    # Create output DataFrame
    output_df = pd.DataFrame(edges)
    output_df.to_csv(output_csv, index=False)

    print(f"Converted {len(edges)} hierarchical relationships to {output_csv}")
    return len(edges)


if __name__ == '__main__':
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'bom.csv'
    output_file = sys.argv[2] if len(sys.argv) > 2 else 'bom_parent_child.csv'

    count = convert_hierarchical_to_parent_child(input_file, output_file)
    print(f"Created {count} parent-child relationships")
