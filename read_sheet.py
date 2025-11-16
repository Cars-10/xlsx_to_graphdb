import pandas as pd
import sys

def read_sheet(file_path, sheet_name):
    """Reads a specific sheet from an Excel file and prints it as CSV."""
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=4)
        print(df.to_csv(index=False))
    except Exception as e:
        print(f"Error reading sheet {sheet_name}: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) > 2:
        read_sheet(sys.argv[1], sys.argv[2])
    else:
        print("Please provide the path to the Excel file and the sheet name.", file=sys.stderr)
        sys.exit(1)