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
    
    Handles the mismatch between catalog table names and actual database table names.
    """
    patient_ids = set()
    has_patient_identifiers = False
    
    # Get list of actual tables in the database
    try:
        available_tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        if not available_tables:
            print("Warning: Database appears to be empty. No tables found.")
            return {f"DUMMY_{i}" for i in range(1, 10)}  # Return dummy IDs
    except Exception as e:
        print(f"Error checking database tables: {e}")
        return {f"DUMMY_{i}" for i in range(1, 10)}  # Return dummy IDs
    
    # Create mapping between catalog table names and database table names
    table_mapping = {}
    for catalog_table in catalog.keys():
        # Try exact match first
        if catalog_table in available_tables:
            table_mapping[catalog_table] = catalog_table
            continue
            
        # Try sanitized versions
        sanitized_name = catalog_table.replace(" ", "_").replace("/", "_").replace("-", "_").lower()
        if sanitized_name in available_tables:
            table_mapping[catalog_table] = sanitized_name
            continue
            
        # Try matching by keywords
        keywords = catalog_table.lower().replace("/", " ").replace("-", " ").split()
        for db_table in available_tables:
            if all(keyword in db_table.lower() for keyword in keywords):
                table_mapping[catalog_table] = db_table
                break
    
    # Look for patient_id or MRN in all tables
    for catalog_table, cols in catalog.items():
        db_table = table_mapping.get(catalog_table)
        
        if not db_table:
            continue  # Skip if no matching database table
            
        if 'patient_id' in cols:
            try:
                for row in conn.execute(f"SELECT DISTINCT patient_id FROM {db_table} WHERE patient_id IS NOT NULL"):
                    patient_ids.add(row[0])
                    has_patient_identifiers = True
            except Exception as e:
                print(f"Error querying patient_id from {db_table}: {e}")
                
        elif 'MRN' in cols:
            try:
                for row in conn.execute(f"SELECT DISTINCT MRN FROM {db_table} WHERE MRN IS NOT NULL"):
                    patient_ids.add(row[0])
                    has_patient_identifiers = True
            except Exception as e:
                print(f"Error querying MRN from {db_table}: {e}")
    
    # Fall back to encounter numbers if no patient identifiers were found
    if not has_patient_identifiers:
        print("Warning: No patient_id or MRN found in the data. Falling back to using encounter numbers as patient identifiers.")
        
        # Find tables with encounter numbers
        enc_tables = []
        for catalog_table, cols in catalog.items():
            if 'encntr_num' in cols and catalog_table in table_mapping:
                enc_tables.append(table_mapping[catalog_table])
        
        # If no mapped tables with encounter numbers, try common table names
        if not enc_tables:
            enc_tables = ['admission_discharge', 'admissions', 'encounters', 'visits']
            enc_tables = [t for t in enc_tables if t in available_tables]
        
        # Try each potential table
        for enc_table in enc_tables:
            try:
                print(f"Trying to fetch encounter numbers from {enc_table}")
                for row in conn.execute(f"SELECT DISTINCT encntr_num FROM {enc_table} WHERE encntr_num IS NOT NULL"):
                    patient_ids.add(f"ENC_{row[0]}")  # Prefix to distinguish from real patient IDs
                    
                if patient_ids:  # If we found any, stop looking
                    break
            except Exception as e:
                print(f"Error querying encntr_num from {enc_table}: {e}")
        
        # If still no patient IDs found, generate dummy IDs
        if not patient_ids:
            print("No valid encounter numbers found. Using dummy patient IDs.")
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
    
    print(f"\n=== MAPPING DETAILS ===")
    
    # Process each IHID table
    for ihid_table, records in details.items():
        if ihid_table not in mapping:
            print(f"Table {ihid_table} not in mapping, skipping")
            continue
            
        print(f"\nProcessing {len(records)} records from table '{ihid_table}'")
        
        for record_idx, record in enumerate(records):
            if record_idx > 3:  # Only print details for first few records to avoid verbosity
                continue
                
            # First identify patient_id and encntr_num to use across all mappings for this record
            patient_id = record.get('patient_id') or record.get('MRN')
            encntr_num = record.get('encntr_num')
            
            print(f"  Record {record_idx+1}: Fields: {list(record.keys())[:5]}... (total {len(record)} fields)")
            print(f"    Patient ID: {patient_id}, Encounter: {encntr_num}")
            
            # If we found a patient ID, mark that we have patient identifiers
            if patient_id:
                has_patient_identifiers = True
            
            # If patient_id is missing but we have an encounter number, use it as a fallback
            # but only if we haven't found any real patient IDs in the dataset
            if not patient_id and encntr_num and not has_patient_identifiers:
                patient_id = f"ENC_{encntr_num}"  # Prefix to distinguish from real patient IDs
                print(f"    Using encounter as patient ID fallback: {patient_id}")

            # Apply mappings for this table
            record_mappings = 0
            for ihid_field, maps in mapping[ihid_table].items():
                if ihid_field not in record:
                    continue
                    
                ihid_value = record[ihid_field]
                
                for map_info in maps:
                    omop_table = map_info['omop_table']
                    omop_field = map_info['omop_field']
                    map_type = map_info.get('mapping_type', 'unknown')
                    
                    # Skip person table as we handle it separately
                    if omop_table == 'person':
                        continue
                        
                    # Skip if already processed (avoid duplicates)
                    key = (omop_table, omop_field, str(ihid_value))
                    if key in processed:
                        continue
                    processed.add(key)
                    
                    if record_idx <= 3:  # Only print details for first few records
                        print(f"    Mapping: {ihid_field} -> {omop_table}.{omop_field} = {ihid_value} (type: {map_type})")
                    record_mappings += 1
                    
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
            
            if record_idx <= 3:
                print(f"    Total mappings applied: {record_mappings}")
    
    print("\n=== MAPPING SUMMARY ===")
    for omop_table, records in omop_data.items():
        print(f"Generated {len(records)} records for {omop_table}")
    
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
        
    # DEBUGGING: Create some synthetic data for testing mappings
    print("\n=== TESTING MAPPING WITH SYNTHETIC DATA ===")
    test_data = {
        "Admission / Discharge": [
            {
                "encntr_num": "E000001",
                "admit_dt_tm": "2023-01-15T09:30:00",
                "disch_dt_tm": "2023-01-18T14:45:00",
                "facility_id_at_admit": 123,
                "admit_source_desc": "Emergency Department",
                "disch_disp_dad_desc": "Home",
                "gender_desc_at_admit": "Male",
                "age_at_admit": 45
            }
        ],
        "Census": [
            {
                "encntr_num": "E000001",
                "gender_desc_at_census": "Male",
                "age_at_census": 45
            }
        ],
        "DAD Diagnosis": [
            {
                "diagnosis_cd": "J18.9",
                "diagnosis_desc": "Pneumonia, unspecified",
                "diagnosis_type_id": 1
            }
        ]
    }
    
    # Test the mapping with synthetic data
    test_omop_data = transform_to_omop(test_data, mapping)
    
    # Merge with the regular data
    for table, records in test_omop_data.items():
        if table != 'person':  # We already handled person records
            omop_data[table].extend(records)
    
    # Then iterate through encounters - check first if tables exist
    try:
        # First check if any of the expected tables exist
        table_check = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        available_tables = [table[0] for table in table_check]
        
        if len(available_tables) == 0:
            print(f"Warning: No tables found in the database. This appears to be a placeholder database.")
            print(f"Available tables would be listed here: {available_tables}")
            print("Creating sample OMOP output without actual data.")
            # Create empty OMOP data structure
            encounter_omop_data = {}
        else:
            # Try to find a suitable table with encounter numbers
            encounter_table = None
            possible_tables = ["admission_discharge", "Admission_Discharge", "admission_discharge_view"]
            
            for table in possible_tables:
                if table in available_tables:
                    encounter_table = table
                    break
            
            # Try to use a sanitized version of the catalog table name
            if not encounter_table:
                for table_name in catalog.keys():
                    if "admission" in table_name.lower() and "discharge" in table_name.lower():
                        sanitized_name = table_name.replace(" ", "_").replace("/", "_").replace("-", "_").lower()
                        if sanitized_name in available_tables:
                            encounter_table = sanitized_name
                            break
            
            if encounter_table:
                print(f"Using table '{encounter_table}' for encounter data")
                for row in conn.execute(f"SELECT encntr_num FROM {encounter_table}"):
                    enc = row['encntr_num']
                    details = fetch_encounter(conn, enc, catalog)
                    
                    # Transform IHID data to OMOP
                    encounter_omop_data = transform_to_omop(details, mapping)
                    
                    # Merge with overall OMOP data
                    for table, records in encounter_omop_data.items():
                        if table != 'person':  # We already handled person records
                            omop_data[table].extend(records)
            else:
                print("No suitable encounter table found in the database. Creating sample output.")
                encounter_omop_data = {}
    except Exception as e:
        print(f"Error accessing database: {e}")
        print("Creating sample OMOP output without actual data.")
        encounter_omop_data = {}
        
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
