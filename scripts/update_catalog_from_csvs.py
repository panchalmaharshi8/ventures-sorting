#!/usr/bin/env python3
"""
Update All_Tables_Combined.json from actual CSV files

This script reads the CSV files in the data directory and generates an updated
All_Tables_Combined.json file that reflects the actual data structure.
"""

import json
import pandas as pd
import os
import glob
from typing import Dict, List, Any

def get_csv_schema(csv_path: str) -> Dict[str, Any]:
    """
    Analyze a CSV file and extract its schema information.
    
    Args:
        csv_path: Path to the CSV file
        
    Returns:
        Dictionary containing column information
    """
    try:
        # Read just the first few rows to get column info
        df = pd.read_csv(csv_path, nrows=5)
        
        schema = {
            'file_path': csv_path,
            'columns': []
        }
        
        for col_name in df.columns:
            # Infer data type from pandas dtype
            dtype = str(df[col_name].dtype)
            
            # Map pandas dtypes to SQL-like types
            if dtype.startswith('int'):
                sql_type = 'INTEGER'
            elif dtype.startswith('float'):
                sql_type = 'DECIMAL'
            elif dtype == 'bool':
                sql_type = 'BOOLEAN'
            elif 'datetime' in dtype:
                sql_type = 'DATETIME'
            else:
                sql_type = 'VARCHAR'
            
            # Try to get some sample values for better understanding
            sample_values = df[col_name].dropna().head(3).tolist()
            
            schema['columns'].append({
                'name': col_name,
                'data_type': sql_type,
                'pandas_dtype': dtype,
                'sample_values': sample_values
            })
            
        return schema
        
    except Exception as e:
        print(f"Error reading {csv_path}: {e}")
        return None

def map_csv_to_source_section(csv_filename: str) -> str:
    """
    Map CSV filenames to appropriate source sections based on IHID naming conventions.
    
    Args:
        csv_filename: Name of the CSV file
        
    Returns:
        Appropriate source section name
    """
    filename = csv_filename.lower()
    
    if 'dad_information' in filename:
        return 'DAD Information'
    elif 'dad_diagnosis' in filename:
        return 'DAD Diagnosis'
    elif 'dad_intervention' in filename or 'dad_interevention' in filename:
        return 'DAD Intervention'
    elif 'admission_discharge' in filename:
        return 'Admission / Discharge'
    elif 'previous_admission' in filename:
        return 'Previous Admission'
    elif 'readmission' in filename:
        return 'Readmission'
    elif 'clinical_event' in filename:
        return 'Clinical Event'
    elif 'lab_result' in filename:
        return 'Laboratory Result'
    elif 'surgery' in filename:
        return 'Surgery'
    else:
        # Generic mapping for unknown files
        return filename.replace('.csv', '').replace('_', ' ').title()

def generate_explanation(column_name: str, source_section: str, sample_values: List) -> str:
    """
    Generate a reasonable explanation for a column based on its name and sample values.
    
    Args:
        column_name: Name of the column
        source_section: Source section the column belongs to
        sample_values: Sample values from the column
        
    Returns:
        Generated explanation string
    """
    col_lower = column_name.lower()
    
    # Common patterns for medical/hospital data
    if 'mrn' in col_lower:
        return "Medical Record Number - unique patient identifier"
    elif 'encntr_num' in col_lower:
        return "Encounter number - unique identifier for a hospital visit/encounter"
    elif 'admit' in col_lower and 'dt_tm' in col_lower:
        return "Date and time of patient admission"
    elif 'disch' in col_lower and 'dt_tm' in col_lower:
        return "Date and time of patient discharge"
    elif 'age' in col_lower:
        return "Patient age at the time of the event"
    elif 'gender' in col_lower:
        return "Patient gender information"
    elif 'diagnosis' in col_lower and 'cd' in col_lower:
        return "Diagnosis code (typically ICD-10)"
    elif 'diagnosis' in col_lower and 'desc' in col_lower:
        return "Diagnosis description"
    elif 'los' in col_lower:
        return "Length of stay in days"
    elif 'nursing_unit' in col_lower:
        return "Nursing unit or ward location"
    elif 'facility' in col_lower:
        return "Healthcare facility identifier or name"
    elif 'event_id' in col_lower:
        return "Unique identifier for a clinical event"
    elif 'result_value' in col_lower:
        return "Numeric or text result value for a clinical measurement"
    elif 'result_interpretation' in col_lower:
        return "Clinical interpretation of a result"
    elif col_lower.endswith('_cd'):
        return f"Code value for {column_name.replace('_cd', '').replace('_', ' ')}"
    elif col_lower.endswith('_desc'):
        return f"Description for {column_name.replace('_desc', '').replace('_', ' ')}"
    elif col_lower.endswith('_dt_tm'):
        return f"Date and time for {column_name.replace('_dt_tm', '').replace('_', ' ')}"
    else:
        # Generic explanation with sample values if available
        sample_str = f" Sample values: {sample_values[:2]}" if sample_values else ""
        return f"Data field from {source_section} table.{sample_str}"

def main():
    """Main function to update the catalog from CSV files."""
    data_dir = 'data'
    output_file = 'All_Tables_Combined.json'
    
    print("Analyzing CSV files in data directory...")
    
    # Find all CSV files in the data directory
    csv_files = glob.glob(os.path.join(data_dir, '*.csv'))
    
    if not csv_files:
        print("No CSV files found in data directory!")
        return
    
    print(f"Found {len(csv_files)} CSV files:")
    for csv_file in csv_files:
        print(f"  - {os.path.basename(csv_file)}")
    
    # Process each CSV file
    all_entries = []
    
    for csv_file in csv_files:
        print(f"\nProcessing: {os.path.basename(csv_file)}")
        schema = get_csv_schema(csv_file)
        
        if not schema:
            continue
        
        # Determine source section
        source_section = map_csv_to_source_section(os.path.basename(csv_file))
        print(f"  Source Section: {source_section}")
        print(f"  Columns: {len(schema['columns'])}")
        
        # Create entries for each column
        for col_info in schema['columns']:
            explanation = generate_explanation(
                col_info['name'], 
                source_section, 
                col_info['sample_values']
            )
            
            entry = {
                "Source_Section": source_section,
                "Column Name": col_info['name'],
                "Data Type": col_info['data_type'],
                "Explanation": explanation
            }
            
            all_entries.append(entry)
    
    # Save the updated catalog
    print(f"\nSaving updated catalog with {len(all_entries)} entries to {output_file}")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_entries, f, indent=2, ensure_ascii=False)
    
    print("All_Tables_Combined.json has been updated successfully!")
    
    # Print summary
    print("\nSummary by Source Section:")
    sections = {}
    for entry in all_entries:
        section = entry['Source_Section']
        if section not in sections:
            sections[section] = 0
        sections[section] += 1
    
    for section, count in sorted(sections.items()):
        print(f"  {section}: {count} columns")

if __name__ == "__main__":
    main()
