#!/usr/bin/env python3
import json
import sqlite3
import os
import random
from collections import defaultdict

def load_catalog(catalog_path):
    """Load the catalog from the JSON file and organize it by table"""
    with open(catalog_path, 'r', encoding='utf-8') as f:
        rows = json.load(f)
    
    # Use a temporary dictionary to track columns and avoid duplicates
    temp_catalog = defaultdict(dict)
    for r in rows:
        tbl = r['Source_Section']
        col = r['Column Name']
        
        # Skip rows with None or empty column names
        if not col:
            continue
            
        data_type = r.get('Data Type')
        if not data_type:
            data_type = 'TEXT'  # Default to TEXT if data type is missing
            
        # Use a dictionary keyed by column name to avoid duplicates
        temp_catalog[tbl][col] = {
            'name': col,
            'type': data_type
        }
    
    # Convert to the final format (dict of lists)
    final_catalog = {}
    for tbl, cols in temp_catalog.items():
        final_catalog[tbl] = list(cols.values())
        
    return final_catalog
    return catalog

def sanitize_column_name(col_name):
    """Clean up problematic column names"""
    if not col_name:
        return "unnamed_column"
    
    # Replace problematic characters
    clean_name = col_name.replace(' ', '_')
    clean_name = clean_name.replace('/', '_')
    clean_name = clean_name.replace('-', '_')
    clean_name = clean_name.replace(':', '')
    clean_name = clean_name.replace('(', '')
    clean_name = clean_name.replace(')', '')
    clean_name = clean_name.replace('.', '_')
    clean_name = clean_name.replace(',', '_')
    
    # Truncate very long names
    if len(clean_name) > 63:
        clean_name = clean_name[:60] + "..."
        
    return clean_name

def create_tables(conn, catalog):
    """Create tables in the database based on catalog structure"""
    for table_name, columns in catalog.items():
        # Sanitize table name for SQL
        sanitized_table = table_name.replace(" ", "_").replace("/", "_").replace("-", "_").lower()
        
        # Construct CREATE TABLE SQL
        col_defs = []
        column_names_seen = set()
        
        for col in columns:
            col_name = col['name']
            
            # Skip empty column names
            if not col_name:
                continue
                
            # Clean up the column name
            clean_col_name = sanitize_column_name(col_name)
            
            # Check for duplicate column names
            if clean_col_name in column_names_seen:
                # Append a unique suffix
                suffix = 1
                while f"{clean_col_name}_{suffix}" in column_names_seen:
                    suffix += 1
                clean_col_name = f"{clean_col_name}_{suffix}"
            
            column_names_seen.add(clean_col_name)
            
            data_type = col['type']
            
            # Map data types to SQLite types
            if data_type.upper() in ['VARCHAR', 'CHARACTER', 'TEXT', 'CHAR', 'STRING']:
                sqlite_type = 'TEXT'
            elif data_type.upper() in ['INTEGER', 'INT', 'SMALLINT']:
                sqlite_type = 'INTEGER'
            elif data_type.upper() in ['DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'REAL']:
                sqlite_type = 'REAL'
            else:
                sqlite_type = 'TEXT'  # Default to TEXT for unknown types
                
            col_defs.append(f"`{clean_col_name}` {sqlite_type}")
        
        # Create the table if it doesn't exist
        create_sql = f"CREATE TABLE IF NOT EXISTS `{sanitized_table}` ({', '.join(col_defs)})"
        try:
            conn.execute(create_sql)
            print(f"Created table: {sanitized_table}")
        except sqlite3.Error as e:
            print(f"Error creating table {sanitized_table}: {e}")

def generate_sample_data(catalog):
    """Generate sample data for each table"""
    sample_data = {}
    
    # Generate 20 unique patient IDs
    patient_ids = [f"P{i:04d}" for i in range(1, 21)]
    
    # Generate 50 unique encounter numbers
    encounter_nums = [f"E{i:06d}" for i in range(1, 51)]
    
    for table_name, columns in catalog.items():
        sanitized_table = table_name.replace(" ", "_").replace("/", "_").replace("-", "_").lower()
        table_rows = []
        
        # Number of rows to generate depends on table
        num_rows = 10
        if 'admission' in table_name.lower() and 'discharge' in table_name.lower():
            num_rows = 50  # More rows for the main encounter table
        
        # Create a mapping of original column names to sanitized column names
        col_mapping = {}
        columns_sanitized = []
        
        for col in columns:
            original_name = col['name']
            if not original_name:
                continue
                
            sanitized_name = sanitize_column_name(original_name)
            col_mapping[original_name] = sanitized_name
            
            # Make a copy with sanitized name
            col_copy = col.copy()
            col_copy['original_name'] = original_name
            col_copy['name'] = sanitized_name
            columns_sanitized.append(col_copy)
        
        for i in range(num_rows):
            row = {}
            
            # Get sanitized column names
            col_names = [col['name'] for col in columns_sanitized]
            original_names = [col['original_name'] for col in columns_sanitized]
            
            # Assign patient_id and MRN - look for these in original names
            patient_id_col = None
            if 'patient_id' in original_names:
                patient_id_col = col_mapping.get('patient_id')
            elif any('patient_id' in name.lower() for name in original_names):
                for orig_name in original_names:
                    if 'patient_id' in orig_name.lower():
                        patient_id_col = col_mapping.get(orig_name)
                        break
                        
            mrn_col = None
            if 'MRN' in original_names:
                mrn_col = col_mapping.get('MRN')
            elif any('mrn' in name.lower() for name in original_names):
                for orig_name in original_names:
                    if 'mrn' in orig_name.lower():
                        mrn_col = col_mapping.get(orig_name)
                        break
            
            if patient_id_col:
                patient_id = random.choice(patient_ids)
                row[patient_id_col] = patient_id
                
                # If we have both patient_id and MRN, make them match
                if mrn_col:
                    row[mrn_col] = patient_id
            elif mrn_col:
                row[mrn_col] = random.choice(patient_ids)
            
            # Assign encounter number for appropriate tables
            encntr_col = None
            if 'encntr_num' in original_names:
                encntr_col = col_mapping.get('encntr_num')
            elif any('encntr_num' in name.lower() for name in original_names):
                for orig_name in original_names:
                    if 'encntr_num' in orig_name.lower():
                        encntr_col = col_mapping.get(orig_name)
                        break
            
            if encntr_col:
                if 'admission' in table_name.lower() and 'discharge' in table_name.lower():
                    # For admission/discharge, use sequential encounters
                    row[encntr_col] = encounter_nums[i % len(encounter_nums)]
                else:
                    # For other tables, randomly assign from existing encounters
                    row[encntr_col] = random.choice(encounter_nums)
            
            # Generate random data for other columns
            for col in columns_sanitized:
                col_name = col['name']
                data_type = col['type']
                
                if col_name in row:  # Skip if already set (patient_id, MRN, encntr_num)
                    continue
                
                # Generate appropriate random data based on type
                if data_type.upper() in ['VARCHAR', 'CHARACTER', 'TEXT', 'CHAR', 'STRING']:
                    row[col_name] = f"Sample_{col_name}_{i}"
                elif data_type.upper() in ['INTEGER', 'INT', 'SMALLINT']:
                    row[col_name] = random.randint(1, 100)
                elif data_type.upper() in ['DECIMAL', 'NUMERIC', 'FLOAT', 'DOUBLE', 'REAL']:
                    row[col_name] = round(random.uniform(1.0, 100.0), 2)
                else:
                    row[col_name] = f"Sample_{col_name}_{i}"
            
            table_rows.append(row)
        
        sample_data[sanitized_table] = table_rows
    
    return sample_data

def insert_sample_data(conn, sample_data):
    """Insert sample data into the database tables"""
    for table_name, rows in sample_data.items():
        if not rows:
            continue
        
        try:
            # Check if the table exists
            table_check = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)).fetchone()
            if not table_check:
                print(f"Table {table_name} does not exist, skipping...")
                continue
                
            # Get actual columns from the database schema
            table_info = conn.execute(f"PRAGMA table_info(`{table_name}`)").fetchall()
            db_columns = [row[1] for row in table_info]  # column name is at index 1
            
            # For each row, filter to only include columns that exist in the table
            for row in rows:
                valid_columns = []
                valid_values = []
                
                for col, val in row.items():
                    if col in db_columns:
                        valid_columns.append(col)
                        valid_values.append(val)
                
                if not valid_columns:
                    continue  # Skip if no valid columns
                
                placeholders = ','.join(['?'] * len(valid_columns))
                insert_sql = f"INSERT INTO `{table_name}` ({','.join(valid_columns)}) VALUES ({placeholders})"
                
                conn.execute(insert_sql, valid_values)
            
            conn.commit()
            print(f"Inserted {len(rows)} rows into table {table_name}")
            
        except sqlite3.Error as e:
            print(f"Error inserting data into {table_name}: {e}")
            conn.rollback()

def main():
    catalog_path = 'All_Tables_Combined.json'
    db_path = 'IHID.db'
    
    # Check if database already exists and ask to overwrite
    if os.path.exists(db_path) and os.path.getsize(db_path) > 0:
        response = input(f"Database {db_path} already exists. Overwrite? (y/n): ")
        if response.lower() != 'y':
            print("Exiting without changes.")
            return
            
    # Load catalog
    catalog = load_catalog(catalog_path)
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    
    # Create tables
    create_tables(conn, catalog)
    
    # Generate and insert sample data
    sample_data = generate_sample_data(catalog)
    insert_sample_data(conn, sample_data)
    
    # Close connection
    conn.close()
    print(f"Sample database created successfully at {db_path}")

if __name__ == '__main__':
    main()
