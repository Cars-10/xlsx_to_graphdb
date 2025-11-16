
import pandas as pd
import sys

def get_sheet_names(file_path):
    """Prints the names of all sheets in an Excel file."""
    try:
        xls = pd.ExcelFile(file_path)
        print("Sheet names:", xls.sheet_names)
    except Exception as e:
        print(f"Error reading sheet names: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        get_sheet_names(sys.argv[1])
    else:
        print("Please provide the path to the Excel file.", file=sys.stderr)
        sys.exit(1)
