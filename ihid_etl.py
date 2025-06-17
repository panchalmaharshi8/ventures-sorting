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

# fetches all patient records from the database
def fetch_all_patients(conn, catalog):
    """Fetch all unique patient IDs from the database.
    If patient_id or MRN are not available, falls back to using encounter numbers.
    """
    patient_ids = set()
    has_patient_identifiers = False
    
    # Look for patient_id or MRN in all tables
    for table, cols in catalog.items():
        if 'patient_id' in cols:
            for row in conn.execute(f"SELECT DISTINCT patient_id FROM {table} WHERE patient_id IS NOT NULL"):
                patient_ids.add(row[0])
                has_patient_identifiers = True
        elif 'MRN' in cols:
            for row in conn.execute(f"SELECT DISTINCT MRN FROM {table} WHERE MRN IS NOT NULL"):
                patient_ids.add(row[0])
                has_patient_identifiers = True
    
    # Fall back to encounter numbers if no patient identifiers were found
    if not has_patient_identifiers:
        print("Warning: No patient_id or MRN found in the data. Falling back to using encounter numbers as patient identifiers.")
        enc_table = "admission_discharge"  # Assumed default table with encounter numbers
        
        # Find the first table that has encntr_num
        for table, cols in catalog.items():
            if 'encntr_num' in cols:
                enc_table = table
                break
                
        try:
            # Check if the table exists first
            table_exists = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (enc_table,)).fetchone()
            
            if table_exists:
                for row in conn.execute(f"SELECT DISTINCT encntr_num FROM {enc_table} WHERE encntr_num IS NOT NULL"):
                    patient_ids.add(f"ENC_{row[0]}")  # Prefix to distinguish from real patient IDs
            else:
                print(f"Warning: Table '{enc_table}' does not exist in the database.")
                # Try alternative tables
                for alt_table in ['admission_discharge', 'encounters', 'visit', 'patients']:
                    try:
                        table_exists = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (alt_table,)).fetchone()
                        if table_exists:
                            print(f"Using alternative table '{alt_table}' for encounter numbers")
                            for row in conn.execute(f"SELECT DISTINCT encntr_num FROM {alt_table} WHERE encntr_num IS NOT NULL"):
                                patient_ids.add(f"ENC_{row[0]}")
                            break
                    except:
                        pass
        except Exception as e:
            print(f"Error fetching encounter numbers: {e}")
            # If all else fails, generate dummy IDs
            patient_ids = {f"DUMMY_{i}" for i in range(1, 10)}
    
    return patient_ids

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
    
    # Track if we found any patient identifiers
    has_patient_identifiers = False
    
    # Process each IHID table
    for ihid_table, records in details.items():
        if ihid_table not in mapping:
            continue
            
        for record in records:
            # First identify patient_id and encntr_num to use across all mappings for this record
            patient_id = record.get('patient_id') or record.get('MRN')
            encntr_num = record.get('encntr_num')
            
            # If we found a patient ID, mark that we have patient identifiers
            if patient_id:
                has_patient_identifiers = True
            
            # If patient_id is missing but we have an encounter number, use it as a fallback
            # but only if we haven't found any real patient IDs in the dataset
            if not patient_id and encntr_num and not has_patient_identifiers:
                patient_id = f"ENC_{encntr_num}"  # Prefix to distinguish from real patient IDs

            # Apply mappings for this table
            for ihid_field, maps in mapping[ihid_table].items():
                if ihid_field not in record:
                    continue
                    
                ihid_value = record[ihid_field]
                
                for map_info in maps:
                    omop_table = map_info['omop_table']
                    omop_field = map_info['omop_field']
                    
                    # Skip person table as we handle it separately
                    if omop_table == 'person':
                        continue
                        
                    # Skip if already processed (avoid duplicates)
                    key = (omop_table, omop_field, str(ihid_value))
                    if key in processed:
                        continue
                    processed.add(key)
                    
                    # Create new OMOP record
                    new_record = {omop_field: ihid_value}
                    
                    # Add standard foreign keys based on table
                    if patient_id:
                        # For all clinical tables, add person_id
                        clinical_tables = [
                            'visit_occurrence', 'condition_occurrence', 'procedure_occurrence', 
                            'drug_exposure', 'measurement', 'observation', 'note', 'specimen'
                        ]
                        if omop_table.lower() in [t.lower() for t in clinical_tables]:
                            new_record['person_id'] = patient_id
                    
                    # Add visit_occurrence_id for tables that need it
                    if encntr_num:
                        event_tables = [
                            'condition_occurrence', 'procedure_occurrence', 'drug_exposure', 
                            'measurement', 'observation', 'note', 'specimen'
                        ]
                        if omop_table.lower() in [t.lower() for t in event_tables]:
                            new_record['visit_occurrence_id'] = encntr_num
                        
                    # Special handling for visit_occurrence to ensure it has correct IDs
                    if omop_table.lower() == 'visit_occurrence':
                        if patient_id and 'person_id' not in new_record:
                            new_record['person_id'] = patient_id
                        if encntr_num and 'visit_occurrence_id' not in new_record:
                            new_record['visit_occurrence_id'] = encntr_num
                    
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
    
    # First, get all unique patients to build the person table
    patients = fetch_all_patients(conn, catalog)
    print(f"Found {len(patients)} unique patients in the database")
    
    # Create a base OMOP data structure with person records
    omop_data = defaultdict(list)
    for patient_id in patients:
        omop_data['person'].append({
            'person_id': patient_id,
            # Add other required person fields with default values since IHID is de-identified
            'gender_concept_id': 0,
            'year_of_birth': 0,
            'race_concept_id': 0,
            'ethnicity_concept_id': 0
        })
    
    # Then iterate through encounters
    for row in conn.execute("SELECT encntr_num FROM admission_discharge"):
        enc = row['encntr_num']
        details = fetch_encounter(conn, enc, catalog)

        # Transform IHID data to OMOP
        encounter_omop_data = transform_to_omop(details, mapping)
        
        # Merge with overall OMOP data
        for table, records in encounter_omop_data.items():
            if table != 'person':  # We already handled person records
                omop_data[table].extend(records)
    
    # Print some statistics on mapping results
    for omop_table, records in omop_data.items():
        print(f"Generated {len(records)} {omop_table} records")
        
    # Save OMOP data to files
    save_omop_data(dict(omop_data), output_dir)

    conn.close()
    print("ETL process completed successfully.")

if __name__ == '__main__':
    main()
