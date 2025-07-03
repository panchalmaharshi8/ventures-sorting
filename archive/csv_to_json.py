import pandas as pd
import os

# Paths
excel_file = "IHID_tables.xlsx"
output_file = "All_Tables_Combined.json"

# 1. Read only the "All_Tables_Combined" sheet
df = pd.read_excel(
    excel_file,
    sheet_name="All_Tables_Combined",
    engine="openpyxl"
)

# 2. Drop columns and rows that are entirely empty
df = df.dropna(axis=1, how="all")
df = df.dropna(axis=0, how="all")

# 3. Export to JSON (one array of objects, one per row)
df.to_json(output_file, orient="records", date_format="iso", indent=2)

# Confirm
print(f"Wrote {len(df)} records to {output_file}")
