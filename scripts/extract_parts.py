
import pandas as pd
import sys
import json

def extract_parts(file_path, sheet_names):
    """Extracts parts from specified sheets in an Excel file and prints them as JSON."""
    all_parts = {}
    for sheet_name in sheet_names:
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=4)
            # The header is duplicated, so we need to get the column names from the first row
            df.columns = df.iloc[0]
            df = df[1:]
            for _, row in df.iterrows():
                part_number = row['Number']
                if pd.notna(part_number):
                    all_parts[part_number] = {
                        'name': row.get('Name'),
                        'type': row.get('Type'),
                        'source': row.get('Source')
                    }
        except Exception as e:
            print(f"Error reading sheet {sheet_name}: {e}", file=sys.stderr)

    print(json.dumps(all_parts, indent=2))

if __name__ == "__main__":
    if len(sys.argv) > 2:
        extract_parts(sys.argv[1], sys.argv[2:])
    else:
        print("Please provide the path to the Excel file and the sheet names.", file=sys.stderr)
        sys.exit(1)
