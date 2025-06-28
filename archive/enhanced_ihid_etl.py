#!/usr/bin/env python3
"""
Enhanced IHID ETL Script for Real CSV Data

This script processes the actual CSV files and transforms them to OMOP format
using the generated mapping file.
"""

import json
import pandas as pd
import os
import sqlite3
import logging
from collections import defaultdict
from typing import Dict, List, Any, Optional
import glob

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class EnhancedIHIDETL:
    def __init__(
        self,
        data_dir: str,
        mapping_path: str,
        output_dir: str
    ):
        """
        Initialize the enhanced ETL processor.
        
        Args:
            data_dir: Directory containing CSV files
            mapping_path: Path to the mapping JSON file
            output_dir: Directory to save OMOP output files
        """
        self.data_dir = data_dir
        self.mapping_path = mapping_path
        self.output_dir = output_dir
        
        # Data structures
        self.mapping = {}
        self.csv_data = {}
        self.omop_data = defaultdict(list)
        
        # Load mapping and CSV data
        self._load_mapping()
        self._load_csv_data()
    
    def _load_mapping(self) -> None:
        """Load the IHID to OMOP mapping from JSON file."""
        logging.info(f"Loading mapping from {self.mapping_path}")
        try:
            with open(self.mapping_path, 'r', encoding='utf-8') as f:
                self.mapping = json.load(f)
            logging.info(f"Loaded mapping for {len(self.mapping)} IHID tables")
        except Exception as e:
            logging.error(f"Error loading mapping: {e}")
            raise
    
    def _load_csv_data(self) -> None:
        """Load all CSV files from the data directory."""
        logging.info(f"Loading CSV files from {self.data_dir}")
        
        # Map CSV files to IHID table names
        csv_to_table_mapping = {
            '1. dad_information.csv': 'DAD Information',
            '2. dad_diagnosis.csv': 'DAD Diagnosis', 
            '3. dad_interevention.csv': 'DAD Intervention',
            '4. admission_discharge.csv': 'Admission / Discharge',
            '5. previous_admission.csv': 'Previous Admission',
            '6. readmission.csv': 'Readmission',
            '7. clinical_event.csv': 'Clinical Event',
            '8. lab_result.csv': 'Laboratory Result',
            '9. surgery.csv': 'Surgery'
        }
        
        for csv_file, table_name in csv_to_table_mapping.items():
            csv_path = os.path.join(self.data_dir, csv_file)
            if os.path.exists(csv_path):
                try:
                    df = pd.read_csv(csv_path)
                    self.csv_data[table_name] = df
                    logging.info(f"Loaded {len(df)} records from {csv_file} as {table_name}")
                except Exception as e:
                    logging.error(f"Error loading {csv_file}: {e}")
            else:
                logging.warning(f"CSV file not found: {csv_path}")
        
        logging.info(f"Loaded {len(self.csv_data)} CSV tables with {sum(len(df) for df in self.csv_data.values())} total records")
    
    def transform_to_omop(self) -> None:
        """Transform IHID data to OMOP format using the mapping."""
        logging.info("Starting IHID to OMOP transformation")
        
        total_mappings_applied = 0
        
        # Process each IHID table
        for ihid_table, df in self.csv_data.items():
            if ihid_table not in self.mapping:
                logging.warning(f"No mapping found for table {ihid_table}")
                continue
            
            logging.info(f"Processing {ihid_table} with {len(df)} records")
            table_mappings = 0
            
            # Process each record in the table
            for idx, record in df.iterrows():
                # Convert pandas Series to dict and handle NaN values
                record_dict = record.to_dict()
                record_dict = {k: (v if pd.notna(v) else None) for k, v in record_dict.items()}
                
                # Apply mappings for this record
                for ihid_field, mappings in self.mapping[ihid_table].items():
                    if ihid_field not in record_dict or record_dict[ihid_field] is None:
                        continue
                    
                    ihid_value = record_dict[ihid_field]
                    
                    # Apply each mapping for this field
                    for mapping_info in mappings:
                        omop_table = mapping_info['omop_table']
                        omop_field = mapping_info['omop_field']
                        
                        # Create or update OMOP record
                        self._add_omop_record(
                            omop_table=omop_table,
                            omop_field=omop_field,
                            value=ihid_value,
                            source_record=record_dict,
                            mapping_info=mapping_info
                        )
                        
                        table_mappings += 1
                        total_mappings_applied += 1
            
            logging.info(f"Applied {table_mappings} mappings for {ihid_table}")
        
        logging.info(f"Transformation complete. Applied {total_mappings_applied} total mappings")
        self._post_process_omop_data()
    
    def _add_omop_record(
        self,
        omop_table: str,
        omop_field: str,
        value: Any,
        source_record: Dict[str, Any],
        mapping_info: Dict[str, Any]
    ) -> None:
        """Add or update an OMOP record with the mapped value."""
        
        # Create a unique identifier for this record based on source identifiers
        record_id = self._generate_record_id(source_record, omop_table)
        
        # Find existing record or create new one
        existing_record = None
        for record in self.omop_data[omop_table]:
            if record.get('_record_id') == record_id:
                existing_record = record
                break
        
        if existing_record is None:
            # Create new record
            new_record = {
                '_record_id': record_id,
                omop_field: value
            }
            
            # Add standard identifiers based on table type
            self._add_standard_identifiers(new_record, source_record, omop_table)
            
            self.omop_data[omop_table].append(new_record)
        else:
            # Update existing record
            existing_record[omop_field] = value
    
    def _generate_record_id(self, source_record: Dict[str, Any], omop_table: str) -> str:
        """Generate a unique record identifier for OMOP records."""
        
        # Get primary identifiers from source record
        mrn = source_record.get('mrn')
        encntr_num = source_record.get('encntr_num')
        event_id = source_record.get('event_id')
        clinical_event_id = source_record.get('clinical_event_id')
        
        # Generate ID based on table type and available identifiers
        if omop_table.lower() == 'person':
            return f"person_{mrn}" if mrn else f"person_unknown_{hash(str(source_record))}"
        elif omop_table.lower() == 'visit_occurrence':
            return f"visit_{encntr_num}" if encntr_num else f"visit_unknown_{hash(str(source_record))}"
        elif omop_table.lower() in ['condition_occurrence', 'procedure_occurrence', 'drug_exposure']:
            base_id = f"{mrn}_{encntr_num}" if mrn and encntr_num else str(hash(str(source_record)))
            return f"{omop_table.lower()}_{base_id}"
        elif event_id or clinical_event_id:
            return f"{omop_table.lower()}_{event_id or clinical_event_id}"
        else:
            # Fallback to hash of the record
            return f"{omop_table.lower()}_{hash(str(source_record))}"
    
    def _add_standard_identifiers(
        self,
        omop_record: Dict[str, Any],
        source_record: Dict[str, Any],
        omop_table: str
    ) -> None:
        """Add standard OMOP identifiers to a record."""
        
        mrn = source_record.get('mrn')
        encntr_num = source_record.get('encntr_num')
        
        # Add person_id for all clinical tables
        if omop_table.lower() != 'person' and mrn:
            omop_record['person_id'] = mrn
        
        # Add visit_occurrence_id for event tables
        event_tables = [
            'condition_occurrence', 'procedure_occurrence', 'drug_exposure',
            'measurement', 'observation', 'device_exposure', 'specimen'
        ]
        if omop_table.lower() in event_tables and encntr_num:
            omop_record['visit_occurrence_id'] = encntr_num
        
        # Add table-specific required fields
        if omop_table.lower() == 'person' and mrn:
            omop_record['person_id'] = mrn
        elif omop_table.lower() == 'visit_occurrence' and encntr_num:
            omop_record['visit_occurrence_id'] = encntr_num
            if mrn:
                omop_record['person_id'] = mrn
    
    def _post_process_omop_data(self) -> None:
        """Post-process OMOP data to ensure consistency and add required fields."""
        logging.info("Post-processing OMOP data")
        
        # Ensure person table exists with all unique patients
        self._ensure_person_table()
        
        # Ensure visit_occurrence table has all unique encounters
        self._ensure_visit_occurrence_table()
        
        # Add sequence numbers for tables that need them
        self._add_sequence_numbers()
        
        # Remove internal record IDs
        for table_name, records in self.omop_data.items():
            for record in records:
                record.pop('_record_id', None)
    
    def _ensure_person_table(self) -> None:
        """Ensure person table contains all unique patients from the data."""
        existing_persons = set()
        for record in self.omop_data.get('person', []):
            if 'person_id' in record:
                existing_persons.add(record['person_id'])
        
        # Find all unique MRNs across all tables
        all_mrns = set()
        for df in self.csv_data.values():
            if 'mrn' in df.columns:
                all_mrns.update(df['mrn'].dropna().unique())
        
        # Add missing persons
        for mrn in all_mrns:
            if mrn not in existing_persons:
                self.omop_data['person'].append({
                    'person_id': mrn,
                    'person_source_value': mrn
                })
        
        logging.info(f"Person table now contains {len(self.omop_data['person'])} unique patients")
    
    def _ensure_visit_occurrence_table(self) -> None:
        """Ensure visit_occurrence table contains all unique encounters."""
        existing_visits = set()
        for record in self.omop_data.get('visit_occurrence', []):
            if 'visit_occurrence_id' in record:
                existing_visits.add(record['visit_occurrence_id'])
        
        # Find all unique encounter numbers across all tables
        all_encounters = set()
        encounter_to_mrn = {}
        
        for df in self.csv_data.values():
            if 'encntr_num' in df.columns:
                for _, row in df.iterrows():
                    if pd.notna(row['encntr_num']):
                        all_encounters.add(row['encntr_num'])
                        if 'mrn' in df.columns and pd.notna(row['mrn']):
                            encounter_to_mrn[row['encntr_num']] = row['mrn']
        
        # Add missing visits
        for encntr_num in all_encounters:
            if encntr_num not in existing_visits:
                visit_record = {
                    'visit_occurrence_id': encntr_num,
                    'visit_source_value': encntr_num
                }
                if encntr_num in encounter_to_mrn:
                    visit_record['person_id'] = encounter_to_mrn[encntr_num]
                
                self.omop_data['visit_occurrence'].append(visit_record)
        
        logging.info(f"Visit_occurrence table now contains {len(self.omop_data['visit_occurrence'])} unique visits")
    
    def _add_sequence_numbers(self) -> None:
        """Add sequence numbers for OMOP tables that require them."""
        
        # Tables that typically need sequence IDs
        sequence_tables = {
            'condition_occurrence': 'condition_occurrence_id',
            'procedure_occurrence': 'procedure_occurrence_id', 
            'drug_exposure': 'drug_exposure_id',
            'measurement': 'measurement_id',
            'observation': 'observation_id',
            'device_exposure': 'device_exposure_id',
            'specimen': 'specimen_id'
        }
        
        for table_name, id_field in sequence_tables.items():
            if table_name in self.omop_data:
                for i, record in enumerate(self.omop_data[table_name], 1):
                    if id_field not in record:
                        record[id_field] = i
    
    def save_omop_data(self) -> None:
        """Save OMOP data to JSON files."""
        logging.info(f"Saving OMOP data to {self.output_dir}")
        os.makedirs(self.output_dir, exist_ok=True)
        
        for table_name, records in self.omop_data.items():
            if records:  # Only save tables with data
                output_path = os.path.join(self.output_dir, f"{table_name.lower()}.json")
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(records, f, indent=2, ensure_ascii=False, default=str)
                logging.info(f"Saved {len(records)} records to {output_path}")
        
        # Save summary
        self._save_transformation_summary()
    
    def _save_transformation_summary(self) -> None:
        """Save a summary of the transformation process."""
        summary = {
            'transformation_summary': {
                'source_tables': len(self.csv_data),
                'source_records': sum(len(df) for df in self.csv_data.values()),
                'omop_tables_generated': len(self.omop_data),
                'omop_records_generated': sum(len(records) for records in self.omop_data.values())
            },
            'source_table_details': {
                table: len(df) for table, df in self.csv_data.items()
            },
            'omop_table_details': {
                table: len(records) for table, records in self.omop_data.items()
            }
        }
        
        summary_path = os.path.join(self.output_dir, 'transformation_summary.json')
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Saved transformation summary to {summary_path}")
    
    def print_summary(self) -> None:
        """Print a summary of the transformation results."""
        print("\\n" + "="*60)
        print("ETL TRANSFORMATION SUMMARY") 
        print("="*60)
        
        total_source_records = sum(len(df) for df in self.csv_data.values())
        total_omop_records = sum(len(records) for records in self.omop_data.values())
        
        print(f"Source tables processed: {len(self.csv_data)}")
        print(f"Total source records: {total_source_records}")
        print(f"OMOP tables generated: {len(self.omop_data)}")
        print(f"Total OMOP records: {total_omop_records}")
        
        print(f"\\nSource table breakdown:")
        for table, df in self.csv_data.items():
            print(f"  {table}: {len(df)} records")
        
        print(f"\\nOMOP table breakdown:")
        for table, records in self.omop_data.items():
            print(f"  {table}: {len(records)} records")


def main():
    """Main function to run the enhanced ETL process."""
    # Configuration
    data_dir = 'data'
    mapping_path = 'ihid_omop_mapping.json'
    output_dir = 'omop_output'
    
    # Verify input files exist
    if not os.path.exists(data_dir):
        logging.error(f"Data directory not found: {data_dir}")
        return
    
    if not os.path.exists(mapping_path):
        logging.error(f"Mapping file not found: {mapping_path}")
        return
    
    # Create ETL processor
    logging.info("Starting enhanced IHID to OMOP ETL process")
    etl = EnhancedIHIDETL(
        data_dir=data_dir,
        mapping_path=mapping_path,
        output_dir=output_dir
    )
    
    # Run transformation
    etl.transform_to_omop()
    etl.save_omop_data()
    etl.print_summary()
    
    logging.info("Enhanced IHID to OMOP ETL process completed successfully")


if __name__ == "__main__":
    main()
