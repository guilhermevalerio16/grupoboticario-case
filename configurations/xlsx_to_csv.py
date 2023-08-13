import sys
import pandas as pd


xlsx_path=sys.argv[1]
csv_filename=sys.argv[2]

def convert_xlsx_to_csv(xlsx_path, csv_filename):
    try:
        df = pd.read_excel(xlsx_path)
    except Exception as e:
        return f"Error reading Excel file: {e}"


    df.to_csv(csv_filename, index=False, header=True)

convert_xlsx_to_csv(xlsx_path, csv_filename)
