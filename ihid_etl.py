import json
import sqlite3
import os
from collections import defaultdict

# loading IHID json
def load_catalog(catalog_path):
    with open(catalog_path, 'r', encoding='utf-8') as f:
        rows = json.load(f)
    catalog = defaultdict(list)
    for r in rows:
        tbl = r['Source_Section']
        col = r['Column Name']
        catalog[tbl].append(col)
    return catalog

#loading mapping json
def load_mapping(mapping_path):
    try:
        with open(mapping_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Warning: Mapping file {mapping_path} not found. No mappings will be applied.")
        return {}  # no mappings yet

# function to fetch any non-null records from the table for a given table and field
def fetch_non_null(conn, table, key_field, key_value, columns):
    col_list = ", ".join(columns)
    sql = f"SELECT {col_list} FROM {table} WHERE {key_field} = ?"
    cursor = conn.execute(sql, (key_value,))
    records = []
    for row in cursor.fetchall():
        # build a sparse dict of only populated fields
        rec = {col: val for col, val in zip(columns, row) if val is not None}
        if rec:
            records.append(rec)
    return records

# fetches everything for a given encounter number, starting from admission/discharge and then other child tables
def fetch_encounter(conn, encntr_num, catalog):
    details = {}
    # First, all tables keyed by encntr_num
    for table, cols in catalog.items():
        # skip DAD_Diagnosis / DAD_Intervention for now; they key on abstract_num
        if table in ('DAD_Diagnosis', 'DAD_Intervention'):
            continue
        if 'encntr_num' not in cols:
            continue
        recs = fetch_non_null(conn, table, 'encntr_num', encntr_num, cols)
        if recs:
            details[table] = recs

    # Now handle CIHI DAD children via abstract_num
    if 'DAD_Abstract' in catalog:
        abs_cols = catalog['DAD_Abstract']
        abstracts = fetch_non_null(conn, 'DAD_Abstract', 'encntr_num', encntr_num, abs_cols)
        if abstracts:
            details['DAD_Abstract'] = abstracts
            for absr in abstracts:
                abs_id = absr.get('abstract_num')
                for child in ('DAD_Diagnosis', 'DAD_Intervention'):
                    child_cols = catalog.get(child, [])
                    if not child_cols or 'abstract_num' not in child_cols:
                        continue
                    recs = fetch_non_null(conn, child, 'abstract_num', abs_id, child_cols)
                    if recs:
                        details.setdefault(child, []).extend(recs)

    return details

def transform_to_omop(details, mapping):
    """
    Transform IHID data to OMOP format based on the provided mapping.
    
    Args:
        details: Dict of IHID tables -> records
        mapping: Dict of mapping from IHID to OMOP
    
    Returns:
        Dict of OMOP tables with their records
    """
    omop_data = defaultdict(list)
    
    # Track mapped fields to avoid duplicates
    processed = set()
    
    # Generate person_id from encounter
    if 'admission_discharge' in details and details['admission_discharge']:
        enc_record = details['admission_discharge'][0]
        if 'encntr_num' in enc_record:
            person_id = enc_record['encntr_num']
            omop_data['person'].append({
                'person_id': person_id,
                # Add other required person fields with default values since IHID is de-identified
                'gender_concept_id': 0,
                'year_of_birth': 0,
                'race_concept_id': 0,
                'ethnicity_concept_id': 0
            })
    
    # Process each IHID table
    for ihid_table, records in details.items():
        if ihid_table not in mapping:
            continue
            
        for record in records:
            # Apply mappings for this table
            for ihid_field, maps in mapping[ihid_table].items():
                if ihid_field not in record:
                    continue
                    
                ihid_value = record[ihid_field]
                
                for map_info in maps:
                    omop_table = map_info['omop_table']
                    omop_field = map_info['omop_field']
                    
                    # Skip if already processed (avoid duplicates)
                    key = (omop_table, omop_field, str(ihid_value))
                    if key in processed:
                        continue
                    processed.add(key)
                    
                    # Create new OMOP record or update existing
                    new_record = {omop_field: ihid_value}
                    
                    # If this is a visit_occurrence, add person_id
                    if omop_table == 'visit_occurrence' and 'encntr_num' in record:
                        new_record['person_id'] = record['encntr_num']
                        new_record['visit_occurrence_id'] = record['encntr_num']
                        
                    # If this is a condition_occurrence, add person_id and visit_occurrence_id
                    if omop_table == 'condition_occurrence' and 'encntr_num' in record:
                        new_record['person_id'] = record['encntr_num']
                        new_record['visit_occurrence_id'] = record['encntr_num']
                        
                    # Add the record to the appropriate OMOP table
                    omop_data[omop_table].append(new_record)
    
    return dict(omop_data)

def save_omop_data(omop_data, output_dir):
    """Save OMOP data to JSON files by table."""
    os.makedirs(output_dir, exist_ok=True)
    
    for table, records in omop_data.items():
        output_path = os.path.join(output_dir, f"{table}.json")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, indent=2)
        print(f"Wrote {len(records)} records to {output_path}")

def main():
    # paths
    catalog_path = 'All_Tables_Combined.json'
    mapping_path = 'ihid_omop_mapping.json'
    db_path      = 'IHID.db'  # TOY DATASET NEEDED HERE
    output_dir = 'omop_output'
    
    # load
    catalog = load_catalog(catalog_path)
    mapping = load_mapping(mapping_path)

    # connect
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # iterate encounters
    for row in conn.execute("SELECT encntr_num FROM admission_discharge"):
        enc = row['encntr_num']
        details = fetch_encounter(conn, enc, catalog)

        # Transform IHID data to OMOP
        omop_data = transform_to_omop(details, mapping)
        
        # Print some statistics on mapping results
        for omop_table, records in omop_data.items():
            print(f"Generated {len(records)} {omop_table} records")
            
        # Save OMOP data to files
        save_omop_data(omop_data, output_dir)

    conn.close()
    print("ETL process completed successfully.")

if __name__ == '__main__':
    main()
