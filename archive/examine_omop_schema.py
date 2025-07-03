#!/usr/bin/env python3
"""
Examine OMOP Schema Excel file to understand the mapping structure
"""

import pandas as pd
import json

def examine_omop_schema():
    """Examine the OMOP schema Excel file"""
    try:
        # Try reading the Excel file
        df = pd.read_excel('OMOP_Summarized_Schema.xlsx')
        
        print("OMOP Schema Excel File Analysis")
        print("=" * 50)
        print(f"Total rows: {len(df)}")
        print(f"Total columns: {len(df.columns)}")
        print("\nColumn names:")
        for i, col in enumerate(df.columns):
            print(f"  {i+1}. {col}")
        
        print("\nFirst 5 rows:")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 50)
        print(df.head())
        
        # Look for specific columns that might contain IHID mappings
        mapping_columns = []
        for col in df.columns:
            if any(keyword in col.lower() for keyword in ['ihid', 'mapping', 'source', 'field']):
                mapping_columns.append(col)
        
        if mapping_columns:
            print(f"\nPotential mapping columns found:")
            for col in mapping_columns:
                print(f"  - {col}")
                
            # Show sample mapping data
            print(f"\nSample mapping data:")
            for col in mapping_columns[:3]:  # Show first 3 mapping columns
                print(f"\n{col}:")
                sample_values = df[col].dropna().head(5).tolist()
                for val in sample_values:
                    print(f"  {val}")
        
        # Check if field names contain source section prefixes
        print(f"\nLooking for field names with source prefixes...")
        field_name_col = None
        for col in df.columns:
            if 'field' in col.lower() and 'name' in col.lower():
                field_name_col = col
                break
        
        if field_name_col:
            print(f"Found field name column: {field_name_col}")
            sample_fields = df[field_name_col].dropna().head(10).tolist()
            
            # Look for fields with source prefixes
            prefixed_fields = []
            for field in sample_fields:
                if isinstance(field, str) and '.' in field:
                    prefixed_fields.append(field)
            
            if prefixed_fields:
                print("Fields with source prefixes found:")
                for field in prefixed_fields:
                    print(f"  {field}")
            else:
                print("No obvious source prefixes found in field names")
        
    except Exception as e:
        print(f"Error reading OMOP schema file: {e}")
        return
    
    # Try to extract any fields that look like they have source table prefixes
    try:
        # Look for any field that starts with known source table names
        source_tables = ['Admission', 'DAD', 'Clinical', 'Lab', 'Surgery']
        
        if field_name_col:
            all_fields = df[field_name_col].dropna().tolist()
            mapped_fields = {}
            
            for field in all_fields:
                if isinstance(field, str):
                    for source in source_tables:
                        if field.startswith(source + '.'):
                            if source not in mapped_fields:
                                mapped_fields[source] = []
                            mapped_fields[source].append(field)
            
            if mapped_fields:
                print(f"\nFields organized by source table:")
                for source, fields in mapped_fields.items():
                    print(f"\n{source} ({len(fields)} fields):")
                    for field in fields[:5]:  # Show first 5 fields
                        print(f"  {field}")
                    if len(fields) > 5:
                        print(f"  ... and {len(fields) - 5} more")
        
    except Exception as e:
        print(f"Error processing field mappings: {e}")

if __name__ == "__main__":
    examine_omop_schema()
