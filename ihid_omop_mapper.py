#!/usr/bin/env python
"""
IHID to OMOP Mapper

This script reads the IHID data structure and maps it to the OMOP Common Data Model,
generating a mapping file that can be used by the ETL process.

In the OMOP CDM, the primary table is PERSON, and we'll map the IHID patient_id
to the OMOP person_id as the root identifier. Although the IHID data is de-identified,
it still maintains unique patient identifiers (patient_id or MRN) across tables.
The encounter numbers (encntr_num) will be mapped to visit_occurrence_id.
"""

import json
import pandas as pd  # type: ignore
import os
import logging
from collections import defaultdict
from typing import Dict, List, Any, Optional, Union

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class IHIDOMOPMapper:
    def __init__(
        self,
        ihid_catalog_path: str,
        omop_schema_path: str,
        output_mapping_path: str
    ):
        """
        Initialize the mapper with paths to required files.
        
        Args:
            ihid_catalog_path: Path to the IHID catalog JSON file
            omop_schema_path: Path to the OMOP schema Excel file
            output_mapping_path: Path to save the generated mapping file
        """
        self.ihid_catalog_path = ihid_catalog_path
        self.omop_schema_path = omop_schema_path
        self.output_mapping_path = output_mapping_path
        
        # Data structures
        self.ihid_catalog = {}
        self.omop_schema = {}
        # Use a nested defaultdict structure
        self.mapping = defaultdict(lambda: defaultdict(list))
        
        # Load data
        self._load_ihid_catalog()
        self._load_omop_schema()
    
    def _load_ihid_catalog(self) -> None:
        """Load the IHID catalog from JSON file."""
        logging.info(f"Loading IHID catalog from {self.ihid_catalog_path}")
        with open(self.ihid_catalog_path, 'r', encoding='utf-8') as f:
            rows = json.load(f)
        
        # Group columns by table
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
        logging.info(f"Loaded {len(self.ihid_catalog)} IHID tables")
    
    def _load_omop_schema(self) -> None:
        """Load the OMOP schema from Excel file."""
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
    
    def generate_mapping(self) -> Dict[str, Any]:
        """Generate the mapping between IHID and OMOP."""
        logging.info("Generating IHID to OMOP mapping")
        
        # First pass - direct mappings from OMOP schema
        for omop_table, fields in self.omop_schema.items():
            for field in fields:
                ihid_table = field['ihid_mapping']['table']
                exact_fields = field['ihid_mapping']['exact_fields']
                non_exact_fields = field['ihid_mapping']['non_exact_fields']
                
                # Skip if no mapping info available
                if not ihid_table and not (exact_fields or non_exact_fields):
                    continue
                
                # Map exact fields first
                for ihid_field in exact_fields:
                    self._add_mapping(
                        ihid_table=ihid_table, 
                        ihid_field=ihid_field,
                        omop_table=omop_table,
                        omop_field=field['name'],
                        mapping_type='exact',
                        description=field['description'],
                        notes=field['ihid_mapping']['notes']
                    )
                
                # Map non-exact fields
                for ihid_field in non_exact_fields:
                    self._add_mapping(
                        ihid_table=ihid_table, 
                        ihid_field=ihid_field,
                        omop_table=omop_table,
                        omop_field=field['name'],
                        mapping_type='non-exact',
                        description=field['description'],
                        notes=field['ihid_mapping']['notes']
                    )
        
        # Special handling for PERSON table since IHID is de-identified
        # Replace with ENCOUNTER_NUMBER as the primary identifier
        self._create_special_person_mapping()
        
        return dict(self.mapping)
    
    def _add_mapping(
        self, 
        ihid_table: Optional[str], 
        ihid_field: str,
        omop_table: str, 
        omop_field: str,
        mapping_type: str,
        description: Optional[str] = None,
        notes: Optional[str] = None
    ) -> None:
        """Add a mapping entry."""
        if not ihid_table:
            logging.warning(f"No IHID table provided for {omop_table}.{omop_field}")
            return
        
        # Some sanity checks
        field_exists = False
        for field_info in self.ihid_catalog.get(ihid_table, []):
            if field_info['name'] == ihid_field:
                field_exists = True
                break
        
        if not field_exists:
            if mapping_type in ['exact', 'non-exact']:
                logging.warning(f"Field {ihid_field} not found in IHID table {ihid_table}")
            else:
                # For special or alternate mappings, we just add a note
                logging.info(f"Adding {mapping_type} mapping for {ihid_field} to {omop_table}.{omop_field} " +
                           f"(field not in current catalog but expected in actual data)")
        
        # Add the mapping information - use defaultdict features
        self.mapping[ihid_table][ihid_field].append({
            'omop_table': omop_table,
            'omop_field': omop_field,
            'mapping_type': mapping_type,
            'description': description,
            'notes': notes
        })
    
    def _create_special_person_mapping(self) -> None:
        """Create special mapping for PERSON table using patient_id or MRN across all tables."""
        logging.info("Creating special mappings for PERSON table with patient_id and visit_occurrence with encntr_num")

        # We'll add anticipated fields to only select tables to prevent validation issues
        # Use admission/discharge table as the primary source for patient identifiers
        target_tables = [table for table in self.ihid_catalog.keys() 
                        if any(name in table.lower() for name in ["admission", "discharge", "abstract", "census"])]
        
        # If no suitable tables found, use the first table
        if not target_tables:
            target_tables = [next(iter(self.ihid_catalog.keys()))]
        
        # Map patient_id to person_id in select tables
        for ihid_table in target_tables:
            self._add_mapping(
                ihid_table=ihid_table,
                ihid_field='patient_id',
                omop_table='PERSON',
                omop_field='person_id',
                mapping_type='anticipated',
                description="Using patient_id as person identifier",
                notes="Although not in current catalog, patient_id is expected in actual data"
            )
            
            # Also add MRN mapping where it should exist
            self._add_mapping(
                ihid_table=ihid_table,
                ihid_field='MRN',
                omop_table='PERSON',
                omop_field='person_id',
                mapping_type='anticipated',
                description="Using MRN as alternate person identifier",
                notes="Documentation mentions both patient_id and MRN as unique patient identifiers"
            )
            
        # Map encounter numbers to visit_occurrence_id across all tables
        for ihid_table, fields in self.ihid_catalog.items():
            if any(field['name'] == 'encntr_num' for field in fields):
                self._add_mapping(
                    ihid_table=ihid_table,
                    ihid_field='encntr_num',
                    omop_table='VISIT_OCCURRENCE',
                    omop_field='visit_occurrence_id',
                    mapping_type='exact',
                    description="Using encounter number as visit identifier",
                    notes="Each encounter represents a unique visit in the OMOP model"
                )
                break
    
    def save_mapping(self) -> None:
        """Save the generated mapping to a JSON file."""
        logging.info(f"Saving mapping to {self.output_mapping_path}")
        with open(self.output_mapping_path, 'w', encoding='utf-8') as f:
            json.dump(dict(self.mapping), f, indent=2)
        logging.info("Mapping saved successfully")
    
    def generate_etl_code(self) -> Optional[str]:
        """Generate ETL code snippets based on the mapping."""
        # This method would generate code snippets for the actual ETL process
        # based on the mappings defined
        # Currently not implemented, will be added in a future version
        return None


def main():
    # Paths
    ihid_catalog_path = 'All_Tables_Combined.json'
    omop_schema_path = 'OMOP_Summarized_Schema.xlsx'
    output_mapping_path = 'ihid_omop_mapping.json'
    
    # Create mapper
    mapper = IHIDOMOPMapper(
        ihid_catalog_path=ihid_catalog_path,
        omop_schema_path=omop_schema_path,
        output_mapping_path=output_mapping_path
    )
    
    # Generate and save mapping
    mapper.generate_mapping()
    mapper.save_mapping()
    
    logging.info("IHID to OMOP mapping process completed successfully")


if __name__ == "__main__":
    main()
