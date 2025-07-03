#!/usr/bin/env python3
import pandas as pd
import json
import os

# --- CONFIGURATION ---

# Path to your Excel schema
EXCEL_PATH = 'OMOP Summarized Schema.xlsx'
# Sheet name (default first sheet)
SHEET_NAME = 0

# Output mapping file
OUTPUT_PATH = 'ihid_omop_mapping.json'

# Prefix decoding map
PREFIX_MAP = {
    'DADAbs': 'DAD Abstract',
    'Admission': 'Admission/Discharge',
    'Emerg': 'Emergency',
    'Ord': 'Order',
    'ActMedServ': 'Activity Med Service',
    'MedIm': 'Medical Imaging',
    'DADDiag': 'DAD Diagnosis',
    'Cens': 'Census',
    'ClinEv': 'Clinical Event',
    'DADInt': 'DAD Intervention',
    'Lab': 'Laboratory Result',
    'DADSCU': 'DAD Special Care Unit',
    'Surg': 'Surgery Case Completed',
    'DIM': 'DIM Clinical Event Code',
    'ActNursUnit' : 'Activity Nursing Unit',
}

# --- UTILS ---

def parse_ihid_fields(cell_value):
    """
    Given a cell from the 'Exact' or 'Non-Exact' column, split into individual entries.
    Handles newlines and commas.
    Returns a list of strings (possibly empty).
    """
    if pd.isna(cell_value):
        return []
    text = str(cell_value)
    # split on newlines first, then commas
    parts = []
    for line in text.splitlines():
        for part in line.split(','):
            part = part.strip()
            if part:
                parts.append(part)
    return parts

# --- MAIN ---

def main():
    # Load the OMOP schema sheet
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
    
    # We'll build mapping[ihid_table][ihid_field] = [ {omop_table, omop_field, mapping_type}, ... ]
    mapping = {}
    
    for _, row in df.iterrows():
        omop_table = row['table_name']
        omop_field = row['field_name']
        
        # Parse lists of IHID entries
        exact_entries    = parse_ihid_fields(row.get('IHID Corresponding Field (Exact)'))
        nonexact_entries = parse_ihid_fields(row.get('IHID Corresponding Fields (Non-Exact)'))
        
        # Helper to process a batch of entries
        def process(entries, mtype):
            for entry in entries:
                # Expect format PREFIX.fieldname
                if '.' not in entry:
                    print(f"⚠️  Skipping malformed entry (no prefix): {entry}")
                    continue
                prefix, fieldname = entry.split('.', 1)
                ihid_table = PREFIX_MAP.get(prefix, None)
                if ihid_table is None:
                    print(f"⚠️  Unknown prefix '{prefix}' for entry '{entry}', using prefix as table name")
                    ihid_table = prefix
                # Initialize
                mapping.setdefault(ihid_table, {})
                mapping[ihid_table].setdefault(fieldname, [])
                # Append mapping info
                mapping[ihid_table][fieldname].append({
                    'omop_table': omop_table,
                    'omop_field': omop_field,
                    'mapping_type': mtype
                })
        
        # Process exact and non-exact
        process(exact_entries,    'exact')
        process(nonexact_entries, 'non-exact')
    
    # Write out JSON
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(mapping, f, indent=2)
    
    print(f"✅ Wrote IHID→OMOP mapping to {OUTPUT_PATH}")

if __name__ == '__main__':
    main()
