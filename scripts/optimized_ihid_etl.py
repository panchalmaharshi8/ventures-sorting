#!/usr/bin/env python3
"""
Optimized IHID to OMOP ETL Pipeline
Processes IHID CSV data and transforms it to OMOP format using efficient lookups.
"""

import json
import logging
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import defaultdict
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class OptimizedIHIDToOMOPETL:
    def __init__(self, data_dir: str = 'data', mapping_file: str = 'ihid_omop_mapping.json'):
        self.data_dir = Path(data_dir)
        self.mapping_file = mapping_file
        self.ihid_data = {}
        self.omop_data = defaultdict(list)
        self.omop_lookup = defaultdict(dict)  # Fast lookup tables
        self.mapping = {}
        
    def load_mapping(self) -> None:
        """Load IHID to OMOP field mappings."""
        try:
            with open(self.mapping_file, 'r') as f:
                raw_mapping = json.load(f)
            
            # Convert nested mapping structure to flat list
            self.mapping = []
            mapping_count = 0
            
            for source_table, field_mappings in raw_mapping.items():
                for ihid_field, omop_mappings in field_mappings.items():
                    if isinstance(omop_mappings, list):
                        for omop_mapping in omop_mappings:
                            if isinstance(omop_mapping, dict):
                                flat_mapping = {
                                    'source_table': source_table,
                                    'ihid_field': ihid_field,
                                    'omop_table': omop_mapping.get('omop_table'),
                                    'omop_field': omop_mapping.get('omop_field'),
                                    'mapping_type': omop_mapping.get('mapping_type'),
                                    'description': omop_mapping.get('description'),
                                    'notes': omop_mapping.get('notes')
                                }
                                self.mapping.append(flat_mapping)
                                mapping_count += 1
            
            logging.info(f"Loaded {mapping_count} mappings from {self.mapping_file} across {len(raw_mapping)} source tables")
        except FileNotFoundError:
            logging.error(f"Mapping file {self.mapping_file} not found")
            raise
        except json.JSONDecodeError as e:
            logging.error(f"Error parsing mapping file: {e}")
            raise
    
    def load_csv_data(self) -> None:
        """Load all CSV files from the data directory."""
        csv_files = list(self.data_dir.glob('*.csv'))
        
        if not csv_files:
            logging.warning(f"No CSV files found in {self.data_dir}")
            return
        
        total_records = 0
        for csv_file in csv_files:
            try:
                # Read CSV with proper handling of encoding and data types
                df = pd.read_csv(csv_file, low_memory=False)
                
                # Clean column names
                df.columns = df.columns.str.strip()
                
                # Convert to records
                records = df.to_dict('records')
                
                # Extract table name from filename
                table_name = csv_file.stem.split('.', 1)[-1].replace('_', ' ').title().strip()
                
                # Fix known naming inconsistencies to match mapping file
                table_name_fixes = {
                    'Dad Information': 'DAD Information',
                    'Dad Diagnosis': 'DAD Diagnosis', 
                    'Dad Interevention': 'DAD Intervention',
                    'Lab Result': 'Laboratory Result',
                    'Admission Discharge': 'Admission / Discharge'
                }
                
                if table_name in table_name_fixes:
                    table_name = table_name_fixes[table_name]
                
                self.ihid_data[table_name] = records
                total_records += len(records)
                
                logging.info(f"Loaded {len(records)} records from {csv_file.name} as {table_name}")
                
            except Exception as e:
                logging.error(f"Error loading {csv_file}: {e}")
                continue
        
        logging.info(f"Loaded {len(self.ihid_data)} CSV tables with {total_records} total records")
    
    def transform_to_omop(self) -> None:
        """Transform IHID data to OMOP format using optimized processing."""
        logging.info("Starting IHID to OMOP transformation")
        
        # Process each IHID table
        for table_name, records in self.ihid_data.items():
            if not records:
                continue
            
            logging.info(f"Processing {table_name} with {len(records)} records")
            start_time = time.time()
            
            # Process records in batches for better performance
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                self._process_batch(table_name, batch)
                
                # Log progress for large tables
                if len(records) > 10000 and (i + batch_size) % 10000 == 0:
                    elapsed = time.time() - start_time
                    progress = (i + batch_size) / len(records) * 100
                    logging.info(f"  Processed {i + batch_size}/{len(records)} records ({progress:.1f}%) in {elapsed:.1f}s")
            
            elapsed = time.time() - start_time
            logging.info(f"Completed {table_name} in {elapsed:.1f}s")
        
        # Post-process the data
        self._post_process_omop_data()
    
    def _process_batch(self, source_table: str, records: List[Dict[str, Any]]) -> None:
        """Process a batch of records efficiently."""
        
        for source_record in records:
            # Standardize source record field names
            source_record = self._standardize_field_names(source_record)
            
            # Find applicable mappings for this table
            applicable_mappings = self._get_applicable_mappings(source_table, source_record)
            
            for mapping in applicable_mappings:
                try:
                    self._apply_mapping_optimized(source_record, mapping)
                except Exception as e:
                    logging.debug(f"Error applying mapping {mapping.get('ihid_field')} -> {mapping.get('omop_table')}.{mapping.get('omop_field')}: {e}")
                    continue
    
    def _apply_mapping_optimized(self, source_record: Dict[str, Any], mapping: Dict[str, Any]) -> None:
        """Apply a single mapping with optimized record handling."""
        ihid_field = mapping['ihid_field']
        omop_table = mapping['omop_table']
        omop_field = mapping['omop_field']
        
        # Get the value from source record
        value = source_record.get(ihid_field)
        if value is None or value == '' or (isinstance(value, float) and pd.isna(value)):
            return
        
        # Convert value based on OMOP field requirements
        converted_value = self._convert_value(value, omop_field, mapping)
        if converted_value is None:
            return
        
        # Generate unique record ID
        record_id = self._generate_record_id(source_record, omop_table)
        
        # Use optimized lookup to find or create record
        if record_id in self.omop_lookup[omop_table]:
            # Update existing record
            record_index = self.omop_lookup[omop_table][record_id]
            self.omop_data[omop_table][record_index][omop_field] = converted_value
        else:
            # Create new record
            new_record = {
                '_record_id': record_id,
                omop_field: converted_value
            }
            
            # Add standard identifiers
            self._add_standard_identifiers(new_record, source_record, omop_table)
            
            # Add to data and lookup
            record_index = len(self.omop_data[omop_table])
            self.omop_data[omop_table].append(new_record)
            self.omop_lookup[omop_table][record_id] = record_index
    
    def _standardize_field_names(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize field names for consistent mapping."""
        standardized = {}
        for key, value in record.items():
            # Convert to lowercase and replace spaces/special chars with underscores
            clean_key = str(key).lower().strip()
            clean_key = clean_key.replace(' ', '_').replace('-', '_').replace('.', '_')
            # Remove duplicate underscores
            while '__' in clean_key:
                clean_key = clean_key.replace('__', '_')
            standardized[clean_key] = value
        return standardized
    
    def _get_applicable_mappings(self, source_table: str, source_record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get mappings applicable to this table and record."""
        applicable = []
        
        for mapping in self.mapping:
            ihid_field = mapping['ihid_field']
            source_table_match = mapping.get('source_table', '')
            
            # Check if mapping applies to this table (exact match, case-insensitive)
            if source_table_match and source_table_match.lower() != source_table.lower():
                continue
            
            # Check if the field exists in the record
            if ihid_field in source_record:
                applicable.append(mapping)
        
        return applicable
    
    def _convert_value(self, value: Any, omop_field: str, mapping: Dict[str, Any]) -> Any:
        """Convert value to appropriate OMOP format."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        
        # Convert based on expected OMOP field type
        field_lower = omop_field.lower()
        
        # ID fields should be integers
        if '_id' in field_lower or field_lower.endswith('_id'):
            try:
                if isinstance(value, str) and value.strip() == '':
                    return None
                return int(float(value))
            except (ValueError, TypeError):
                return None
        
        # Date fields
        if 'date' in field_lower or 'datetime' in field_lower:
            return self._convert_to_date(value)
        
        # Numeric fields
        if 'amount' in field_lower or 'value' in field_lower or 'quantity' in field_lower:
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        # String fields - clean and standardize
        if isinstance(value, str):
            cleaned = value.strip()
            return cleaned if cleaned else None
        
        return value
    
    def _convert_to_date(self, value: Any) -> Optional[str]:
        """Convert various date formats to OMOP standard (YYYY-MM-DD)."""
        if not value or (isinstance(value, float) and pd.isna(value)):
            return None
        
        try:
            # Try to parse with pandas
            date_obj = pd.to_datetime(value, errors='coerce')
            if pd.isna(date_obj):
                return None
            return date_obj.strftime('%Y-%m-%d')
        except:
            return None
    
    def _generate_record_id(self, source_record: Dict[str, Any], omop_table: str) -> str:
        """Generate a unique record identifier for OMOP records."""
        
        # Get primary identifiers from source record
        mrn = source_record.get('mrn') or source_record.get('medical_record_number')
        encntr_num = source_record.get('encntr_num') or source_record.get('encounter_number')
        event_id = source_record.get('event_id') or source_record.get('clinical_event_id')
        
        # Generate ID based on table type and available identifiers
        if omop_table.lower() == 'person':
            return f"person_{mrn}" if mrn else f"person_unknown_{hash(str(source_record))}"
        elif omop_table.lower() == 'visit_occurrence':
            return f"visit_{encntr_num}" if encntr_num else f"visit_unknown_{hash(str(source_record))}"
        elif omop_table.lower() in ['condition_occurrence', 'procedure_occurrence', 'drug_exposure']:
            base_id = f"{mrn}_{encntr_num}" if mrn and encntr_num else str(hash(str(source_record)))
            return f"{omop_table.lower()}_{base_id}"
        elif event_id:
            return f"{omop_table.lower()}_{event_id}"
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
        
        mrn = source_record.get('mrn') or source_record.get('medical_record_number')
        encntr_num = source_record.get('encntr_num') or source_record.get('encounter_number')
        
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
        
        # Remove internal record IDs and lookup tables
        for table_name, records in self.omop_data.items():
            for record in records:
                record.pop('_record_id', None)
        
        # Clear lookup tables to free memory
        self.omop_lookup.clear()
        
        # Log final statistics
        total_records = sum(len(records) for records in self.omop_data.values())
        logging.info(f"Generated {len(self.omop_data)} OMOP tables with {total_records} total records")
        
        for table_name, records in sorted(self.omop_data.items()):
            logging.info(f"  {table_name}: {len(records)} records")
    
    def save_omop_data(self, output_dir: str = 'omop_output') -> None:
        """Save OMOP data to JSON files."""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        logging.info(f"Saving OMOP data to {output_dir}")
        
        for table_name, records in self.omop_data.items():
            if not records:
                continue
            
            output_file = output_path / f"{table_name.lower()}.json"
            try:
                with open(output_file, 'w') as f:
                    json.dump(records, f, indent=2, default=str)
                logging.info(f"Saved {len(records)} records to {output_file}")
            except Exception as e:
                logging.error(f"Error saving {output_file}: {e}")
    
    def run_etl(self) -> None:
        """Run the complete ETL pipeline."""
        start_time = time.time()
        
        try:
            logging.info("Starting IHID to OMOP ETL pipeline")
            
            # Load configuration and data
            self.load_mapping()
            self.load_csv_data()
            
            if not self.ihid_data:
                logging.error("No IHID data loaded. Exiting.")
                return
            
            # Transform data
            self.transform_to_omop()
            
            # Save results
            self.save_omop_data()
            
            elapsed = time.time() - start_time
            logging.info(f"ETL pipeline completed successfully in {elapsed:.1f} seconds")
            
        except Exception as e:
            logging.error(f"ETL pipeline failed: {e}")
            raise

def main():
    """Main entry point."""
    etl = OptimizedIHIDToOMOPETL()
    etl.run_etl()

if __name__ == "__main__":
    main()
