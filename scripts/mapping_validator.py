#!/usr/bin/env python
"""
IHID to OMOP Mapping Validator

This script helps validate the mappings created between IHID and OMOP by
analyzing the coverage and potential data quality issues.
"""

import json
import pandas as pd
import os
import sys
import logging
from typing import Dict, Any, List, Set

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MappingValidator:
    def __init__(self, mapping_path: str, ihid_catalog_path: str, omop_schema_path: str):
        """Initialize the validator."""
        self.mapping_path = mapping_path
        self.ihid_catalog_path = ihid_catalog_path
        self.omop_schema_path = omop_schema_path
        
        # Load data
        self.mapping = self._load_mapping()
        self.ihid_catalog = self._load_ihid_catalog()
        self.omop_schema = self._load_omop_schema()
    
    def _load_mapping(self) -> Dict[str, Any]:
        """Load the mapping from JSON file."""
        try:
            with open(self.mapping_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logging.error(f"Mapping file {self.mapping_path} not found")
            return {}
    
    def _load_ihid_catalog(self) -> Dict[str, List[Dict[str, Any]]]:
        """Load the IHID catalog."""
        try:
            with open(self.ihid_catalog_path, 'r', encoding='utf-8') as f:
                rows = json.load(f)
            
            catalog = {}
            for row in rows:
                table = row.get('Source_Section')
                col = row.get('Column Name')
                if table and col:
                    if table not in catalog:
                        catalog[table] = []
                    catalog[table].append({
                        'name': col,
                        'type': row.get('Data Type'),
                        'explanation': row.get('Explanation')
                    })
            return catalog
        except FileNotFoundError:
            logging.error(f"IHID catalog file {self.ihid_catalog_path} not found")
            return {}
    
    def _load_omop_schema(self) -> Dict[str, List[Dict[str, Any]]]:
        """Load the OMOP schema."""
        try:
            df = pd.read_excel(self.omop_schema_path)
            
            schema = {}
            for _, row in df.iterrows():
                table_name = row.get('table_name')
                field_name = row.get('field_name')
                
                if not table_name or not field_name or pd.isna(table_name) or pd.isna(field_name):
                    continue
                
                if table_name not in schema:
                    schema[table_name] = []
                
                schema[table_name].append({
                    'name': field_name,
                    'description': row.get('description')
                })
            return schema
        except FileNotFoundError:
            logging.error(f"OMOP schema file {self.omop_schema_path} not found")
            return {}
        except Exception as e:
            logging.error(f"Error loading OMOP schema: {e}")
            return {}
    
    def validate_coverage(self) -> Dict[str, Any]:
        """Validate the coverage of the mapping."""
        results = {
            'ihid_tables': {
                'total': len(self.ihid_catalog),
                'mapped': 0,
                'unmapped': []
            },
            'ihid_fields': {
                'total': 0,
                'mapped': 0,
                'unmapped_by_table': {}
            },
            'omop_tables': {
                'total': len(self.omop_schema),
                'mapped': 0,
                'unmapped': []
            },
            'omop_fields': {
                'total': 0,
                'mapped': 0,
                'unmapped_by_table': {}
            }
        }
        
        # Check IHID coverage
        mapped_ihid_tables = set(self.mapping.keys())
        all_ihid_tables = set(self.ihid_catalog.keys())
        
        # Calculate the properly mapped tables count
        valid_mapped_tables = mapped_ihid_tables.intersection(all_ihid_tables)
        results['ihid_tables']['mapped'] = len(valid_mapped_tables)
        results['ihid_tables']['unmapped'] = sorted(list(all_ihid_tables - valid_mapped_tables))
        
        # Check IHID fields coverage
        mapped_ihid_fields = set()
        total_ihid_fields = 0
        
        for table, fields in self.ihid_catalog.items():
            table_fields = set(field['name'] for field in fields)
            total_ihid_fields += len(table_fields)
            
            if table in self.mapping:
                mapped_table_fields = set(self.mapping[table].keys())
                mapped_ihid_fields.update(mapped_table_fields)
                unmapped_fields = table_fields - mapped_table_fields
                if unmapped_fields:
                    results['ihid_fields']['unmapped_by_table'][table] = sorted(list(unmapped_fields))
            else:
                # All fields in this table are unmapped
                results['ihid_fields']['unmapped_by_table'][table] = sorted(list(table_fields))
        
        results['ihid_fields']['total'] = total_ihid_fields
        results['ihid_fields']['mapped'] = len(mapped_ihid_fields)
        
        # Check OMOP coverage
        mapped_omop_tables = set()
        mapped_omop_fields = set()
        total_omop_fields = 0
        
        # Extract all mapped OMOP tables and fields
        for ihid_table, ihid_fields in self.mapping.items():
            for ihid_field, mappings in ihid_fields.items():
                for map_info in mappings:
                    omop_table = map_info.get('omop_table')
                    omop_field = map_info.get('omop_field')
                    if omop_table and omop_field:
                        mapped_omop_tables.add(omop_table)
                        mapped_omop_fields.add((omop_table, omop_field))
        
        # Calculate OMOP coverage
        all_omop_tables = set(self.omop_schema.keys())
        unmapped_omop_tables = all_omop_tables - mapped_omop_tables
        
        results['omop_tables']['mapped'] = len(mapped_omop_tables)
        results['omop_tables']['unmapped'] = sorted(list(unmapped_omop_tables))
        
        # Check OMOP fields coverage
        for table, fields in self.omop_schema.items():
            table_total_fields = len(fields)
            total_omop_fields += table_total_fields
            
            table_fields = set(field['name'] for field in fields)
            mapped_table_fields = set(field for omop_table, field in mapped_omop_fields if omop_table == table)
            unmapped_fields = table_fields - mapped_table_fields
            
            if unmapped_fields:
                results['omop_fields']['unmapped_by_table'][table] = sorted(list(unmapped_fields))
        
        results['omop_fields']['total'] = total_omop_fields
        results['omop_fields']['mapped'] = len(mapped_omop_fields)
        
        return results
    
    def print_validation_results(self, results: Dict[str, Any]) -> None:
        """Print validation results in a readable format."""
        print("\n=== IHID to OMOP Mapping Validation ===\n")
        
        # IHID coverage
        print(f"IHID Tables: {results['ihid_tables']['mapped']}/{results['ihid_tables']['total']} mapped ({round(results['ihid_tables']['mapped']/results['ihid_tables']['total']*100, 1)}%)")
        
        if results['ihid_tables']['unmapped']:
            print(f"  Unmapped IHID Tables ({len(results['ihid_tables']['unmapped'])}):")
            for table in sorted(results['ihid_tables']['unmapped']):
                print(f"    - {table}")
        
        # IHID fields
        print(f"\nIHID Fields: {results['ihid_fields']['mapped']}/{results['ihid_fields']['total']} mapped ({round(results['ihid_fields']['mapped']/results['ihid_fields']['total']*100, 1)}%)")
        
        # OMOP coverage
        print(f"\nOMOP Tables: {results['omop_tables']['mapped']}/{results['omop_tables']['total']} mapped ({round(results['omop_tables']['mapped']/results['omop_tables']['total']*100, 1)}%)")
        
        if results['omop_tables']['unmapped']:
            print(f"  Unmapped OMOP Tables ({len(results['omop_tables']['unmapped'])}):")
            for table in sorted(results['omop_tables']['unmapped']):
                print(f"    - {table}")
        
        # OMOP fields
        print(f"\nOMOP Fields: {results['omop_fields']['mapped']}/{results['omop_fields']['total']} mapped ({round(results['omop_fields']['mapped']/results['omop_fields']['total']*100, 1)}%)")
        
        # Tables with most unmapped fields
        print("\nTop OMOP Tables with Most Unmapped Fields:")
        omop_unmapped = [(table, len(fields)) for table, fields in results['omop_fields']['unmapped_by_table'].items()]
        for table, count in sorted(omop_unmapped, key=lambda x: x[1], reverse=True)[:5]:
            print(f"  - {table}: {count} unmapped fields")


def main():
    if len(sys.argv) < 3:
        print("Usage: python mapping_validator.py <mapping_file> <ihid_catalog> <omop_schema>")
        print("Example: python mapping_validator.py ihid_omop_mapping.json All_Tables_Combined.json OMOP_Summarized_Schema.xlsx")
        sys.exit(1)
    
    mapping_path = sys.argv[1]
    ihid_catalog_path = sys.argv[2]
    omop_schema_path = sys.argv[3]
    
    validator = MappingValidator(mapping_path, ihid_catalog_path, omop_schema_path)
    results = validator.validate_coverage()
    validator.print_validation_results(results)


if __name__ == "__main__":
    main()
