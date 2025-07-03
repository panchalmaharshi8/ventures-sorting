#!/usr/bin/env python3
"""
Enhanced IHID to OMOP Mapper that works with real CSV data and proper field mappings

This script reads the actual CSV data structure and the OMOP schema Excel file
with field mappings that use source table prefixes (e.g., Admission.field_name).
"""

import json
import pandas as pd
import os
import logging
from collections import defaultdict
from typing import Dict, List, Any, Optional, Union

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EnhancedIHIDOMOPMapper:
    def __init__(
        self,
        ihid_catalog_path: str,
        omop_schema_path: str,
        output_mapping_path: str
    ):
        """
        Initialize the enhanced mapper with paths to required files.
        
        Args:
            ihid_catalog_path: Path to the IHID catalog JSON file (generated from CSVs)
            omop_schema_path: Path to the OMOP schema Excel file
            output_mapping_path: Path to save the generated mapping file
        """
        self.ihid_catalog_path = ihid_catalog_path
        self.omop_schema_path = omop_schema_path
        self.output_mapping_path = output_mapping_path
        
        # Data structures
        self.ihid_catalog = {}
        self.omop_schema = {}
        self.mapping = defaultdict(lambda: defaultdict(list))
        
        # Load data
        self._load_ihid_catalog()
        self._load_omop_schema()
    
    def _load_ihid_catalog(self) -> None:
        """Load the IHID catalog from JSON file generated from CSV analysis."""
        logging.info(f"Loading IHID catalog from {self.ihid_catalog_path}")
        with open(self.ihid_catalog_path, 'r', encoding='utf-8') as f:
            rows = json.load(f)
        
        # Group columns by table (Source_Section)
        self.ihid_catalog = defaultdict(list)
        for row in rows:
            table = row.get('Source_Section')
            col = row.get('Column Name')
            if table and col:
                self.ihid_catalog[table].append({
                    'name': col,
                    'type': row.get('Data Type'),
                    'explanation': row.get('Explanation')
                })
        logging.info(f"Loaded {len(self.ihid_catalog)} IHID tables with {sum(len(cols) for cols in self.ihid_catalog.values())} total columns")
    
    def _load_omop_schema(self) -> None:
        """Load the OMOP schema from Excel file with proper field mapping parsing."""
        logging.info(f"Loading OMOP schema from {self.omop_schema_path}")
        try:
            df = pd.read_excel(self.omop_schema_path)
            
            # Process the schema into a more convenient format
            for _, row in df.iterrows():
                table_name = row.get('table_name')
                field_name = row.get('field_name')
                
                if not table_name or not field_name or pd.isna(table_name) or pd.isna(field_name):
                    continue
                
                # Extract IHID mapping information
                ihid_table = row.get('IHID Corresponding Table')
                ihid_exact_fields = row.get('IHID Corresponding Field (Exact)')
                ihid_non_exact_fields = row.get('IHID Corresponding Fields (Non-Exact)')
                notes = row.get('Notes')
                
                # Store structured mapping info
                if table_name not in self.omop_schema:
                    self.omop_schema[table_name] = []
                
                field_info = {
                    'name': field_name,
                    'description': row.get('description'),
                    'ihid_mapping': {
                        'table': ihid_table if not pd.isna(ihid_table) else None,
                        'exact_fields': self._parse_field_list(ihid_exact_fields),
                        'non_exact_fields': self._parse_field_list(ihid_non_exact_fields),
                        'notes': notes if not pd.isna(notes) else None
                    }
                }
                
                self.omop_schema[table_name].append(field_info)
            
            logging.info(f"Loaded {len(self.omop_schema)} OMOP tables")
        except Exception as e:
            logging.error(f"Error loading OMOP schema: {e}")
            raise
    
    def _parse_field_list(self, field_str: Union[str, float, None]) -> List[str]:
        """Parse a list of fields from a string with newlines or commas."""
        if pd.isna(field_str) or not field_str:
            return []
        
        # Handle both newline and comma-separated lists
        if isinstance(field_str, str):
            fields = []
            for line in field_str.split('\n'):
                for field in line.split(','):
                    field = field.strip()
                    if field:
                        fields.append(field)
            return fields
        return []
    
    def _parse_source_field(self, field_with_source: str) -> tuple:
        """
        Parse a field that has source table prefix (e.g., 'Admission.field_name').
        
        Args:
            field_with_source: Field name with source prefix
            
        Returns:
            Tuple of (source_table, field_name)
        """
        if '.' in field_with_source:
            parts = field_with_source.split('.', 1)
            source_table = parts[0].strip()
            field_name = parts[1].strip()
            
            # Map source table names to actual IHID catalog table names
            source_mapping = {
                'Admission': 'Admission / Discharge',
                'DADAbs': 'DAD Information',
                'DADDx': 'DAD Diagnosis', 
                'DADInt': 'DAD Intervention',
                'Clinical': 'Clinical Event',
                'Lab': 'Laboratory Result',
                'Surgery': 'Surgery',
                'Readm': 'Readmission',
                'PrevAdm': 'Previous Admission'
            }
            
            # Try exact match first, then partial matches
            mapped_source = None
            for abbrev, full_name in source_mapping.items():
                if source_table == abbrev or source_table.startswith(abbrev):
                    mapped_source = full_name
                    break
            
            # If no mapping found, try to find best match in catalog
            if not mapped_source:
                for catalog_table in self.ihid_catalog.keys():
                    if source_table.lower() in catalog_table.lower():
                        mapped_source = catalog_table
                        break
                        
            return mapped_source or source_table, field_name
        else:
            return None, field_with_source
    
    def generate_mapping(self) -> Dict[str, Any]:
        """Generate the mapping between IHID and OMOP using real field mappings."""
        logging.info("Generating IHID to OMOP mapping from Excel schema")
        
        total_mappings = 0
        
        # Process mappings from OMOP schema
        for omop_table, fields in self.omop_schema.items():
            for field in fields:
                exact_fields = field['ihid_mapping']['exact_fields']
                non_exact_fields = field['ihid_mapping']['non_exact_fields']
                
                # Skip if no mapping info available
                if not (exact_fields or non_exact_fields):
                    continue
                
                # Process exact field mappings
                for ihid_field_with_source in exact_fields:
                    source_table, field_name = self._parse_source_field(ihid_field_with_source)
                    if source_table and field_name:
                        success = self._add_mapping(
                            ihid_table=source_table,
                            ihid_field=field_name,
                            omop_table=omop_table,
                            omop_field=field['name'],
                            mapping_type='exact',
                            description=field['description'],
                            notes=field['ihid_mapping']['notes']
                        )
                        if success:
                            total_mappings += 1
                
                # Process non-exact field mappings
                for ihid_field_with_source in non_exact_fields:
                    source_table, field_name = self._parse_source_field(ihid_field_with_source)
                    if source_table and field_name:
                        success = self._add_mapping(
                            ihid_table=source_table,
                            ihid_field=field_name,
                            omop_table=omop_table,
                            omop_field=field['name'],
                            mapping_type='non-exact',
                            description=field['description'],
                            notes=field['ihid_mapping']['notes']
                        )
                        if success:
                            total_mappings += 1
        
        # Add special mappings for core identifiers that should exist
        self._create_core_identifier_mappings()
        
        logging.info(f"Generated {total_mappings} field mappings")
        return dict(self.mapping)
    
    def _add_mapping(
        self,
        ihid_table: str,
        ihid_field: str,
        omop_table: str,
        omop_field: str,
        mapping_type: str,
        description: Optional[str] = None,
        notes: Optional[str] = None
    ) -> bool:
        """Add a mapping entry and return True if successful."""
        
        # Check if the IHID table exists in our catalog
        if ihid_table not in self.ihid_catalog:
            # Try to find a close match
            for catalog_table in self.ihid_catalog.keys():
                if ihid_table.lower() in catalog_table.lower() or catalog_table.lower() in ihid_table.lower():
                    logging.info(f"Mapping table '{ihid_table}' to '{catalog_table}'")
                    ihid_table = catalog_table
                    break
            else:
                logging.warning(f"IHID table '{ihid_table}' not found in catalog")
                return False
        
        # Check if the field exists in the table
        field_exists = False
        for field_info in self.ihid_catalog[ihid_table]:
            if field_info['name'] == ihid_field:
                field_exists = True
                break
        
        if not field_exists:
            if mapping_type in ['exact']:
                logging.warning(f"Field '{ihid_field}' not found in table '{ihid_table}'")
                return False
            else:
                logging.info(f"Adding {mapping_type} mapping for {ihid_field} (field not in current data but may exist)")
        
        # Add the mapping
        self.mapping[ihid_table][ihid_field].append({
            'omop_table': omop_table,
            'omop_field': omop_field,
            'mapping_type': mapping_type,
            'description': description,
            'notes': notes
        })
        
        return True
    
    def _create_core_identifier_mappings(self) -> None:
        """Create mappings for core identifiers that should be present in the data."""
        logging.info("Adding core identifier mappings")
        
        # Find tables that contain MRN and encntr_num (our core identifiers)
        for table_name, fields in self.ihid_catalog.items():
            field_names = [f['name'] for f in fields]
            
            # Map MRN to person_id in PERSON table
            if 'mrn' in field_names:
                self.mapping[table_name]['mrn'].append({
                    'omop_table': 'person',
                    'omop_field': 'person_id',
                    'mapping_type': 'core_identifier',
                    'description': 'Medical Record Number as person identifier',
                    'notes': 'Primary patient identifier'
                })
            
            # Map encntr_num to visit_occurrence_id in VISIT_OCCURRENCE table
            if 'encntr_num' in field_names:
                self.mapping[table_name]['encntr_num'].append({
                    'omop_table': 'visit_occurrence',
                    'omop_field': 'visit_occurrence_id',
                    'mapping_type': 'core_identifier',
                    'description': 'Encounter number as visit identifier',
                    'notes': 'Primary visit/encounter identifier'
                })
    
    def save_mapping(self) -> None:
        """Save the generated mapping to a JSON file."""
        logging.info(f"Saving mapping to {self.output_mapping_path}")
        
        # Convert to regular dict for JSON serialization
        mapping_dict = {}
        for table, fields in self.mapping.items():
            mapping_dict[table] = {}
            for field, mappings in fields.items():
                mapping_dict[table][field] = mappings
        
        with open(self.output_mapping_path, 'w', encoding='utf-8') as f:
            json.dump(mapping_dict, f, indent=2, ensure_ascii=False)
        
        logging.info("Mapping saved successfully")
    
    def print_mapping_summary(self) -> None:
        """Print a summary of the generated mappings."""
        print("\n" + "="*60)
        print("MAPPING SUMMARY")
        print("="*60)
        
        total_tables = len(self.mapping)
        total_fields = sum(len(fields) for fields in self.mapping.values())
        total_mappings = sum(len(mappings) for fields in self.mapping.values() for mappings in fields.values())
        
        print(f"Total IHID tables mapped: {total_tables}")
        print(f"Total IHID fields mapped: {total_fields}")
        print(f"Total OMOP mappings created: {total_mappings}")
        
        print(f"\nMappings by IHID table:")
        for table_name, fields in self.mapping.items():
            field_count = len(fields)
            mapping_count = sum(len(mappings) for mappings in fields.values())
            print(f"  {table_name}: {field_count} fields, {mapping_count} mappings")
        
        # Show OMOP tables being mapped to
        omop_tables = set()
        for fields in self.mapping.values():
            for mappings in fields.values():
                for mapping in mappings:
                    omop_tables.add(mapping['omop_table'])
        
        print(f"\nOMOP tables being populated ({len(omop_tables)}):")
        for table in sorted(omop_tables):
            print(f"  - {table}")


def main():
    """Main function to run the enhanced mapping process."""
    # Paths
    ihid_catalog_path = 'All_Tables_Combined.json'
    omop_schema_path = 'OMOP_Summarized_Schema.xlsx'
    output_mapping_path = 'ihid_omop_mapping.json'
    
    # Verify input files exist
    for file_path in [ihid_catalog_path, omop_schema_path]:
        if not os.path.exists(file_path):
            logging.error(f"Required file not found: {file_path}")
            return
    
    # Create enhanced mapper
    logging.info("Starting enhanced IHID to OMOP mapping process")
    mapper = EnhancedIHIDOMOPMapper(
        ihid_catalog_path=ihid_catalog_path,
        omop_schema_path=omop_schema_path,
        output_mapping_path=output_mapping_path
    )
    
    # Generate and save mapping
    mapper.generate_mapping()
    mapper.save_mapping()
    mapper.print_mapping_summary()
    
    logging.info("Enhanced IHID to OMOP mapping process completed successfully")


if __name__ == "__main__":
    main()
