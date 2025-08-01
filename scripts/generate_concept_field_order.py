#!/usr/bin/env python3

import json
import pandas as pd
from pathlib import Path
from collections import defaultdict

SCHEMA_PATH = Path("OMOP Summarized Schema.xlsx")
OUTPUT_PATH = Path("schemas/concept_field_order.json")

def generate_field_order():
    if not SCHEMA_PATH.exists():
        raise FileNotFoundError(f"Missing schema file at {SCHEMA_PATH}")
    
    df = pd.read_excel(SCHEMA_PATH)

    # Filter for *_concept_id fields (case-insensitive), excluding the 'concept' table
    concept_rows = df[
        df['field_name'].str.contains('concept_id', case=False, na=False) &
        (~df['field_name'].str.lower().str.endswith('_source_concept_id')) &
        (~df['field_name'].str.lower().str.endswith('_type_concept_id')) &
        (df['table_name'].str.lower() != 'concept')
    ]

    # Build field order mapping
    field_order_map = defaultdict(dict)
    table_counters = defaultdict(int)

    for _, row in concept_rows.iterrows():
        table = row['table_name']
        field = row['field_name']

        table_counters[table] += 1
        field_order_map[table][field] = table_counters[table]

    # Write to JSON
    with open(OUTPUT_PATH, "w") as f:
        json.dump(field_order_map, f, indent=2)

    print(f"Saved concept field order map to {OUTPUT_PATH}")

if __name__ == "__main__":
    generate_field_order()
