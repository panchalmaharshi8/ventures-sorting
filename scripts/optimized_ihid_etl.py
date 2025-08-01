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
from typing import Tuple

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
        self.place_of_service_lookup = {}
        self.next_concept_id = 1000000
        self.concept_id_counters = defaultdict(lambda: defaultdict(dict))  # [table][field][value] = concept_id
        self.concept_field_order = {}  # Loaded from JSON
        self.table_ids = {}  # Loaded from table_ids.txt
        
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
                # Try reading as comma-delimited first
                try:
                    df = pd.read_csv(csv_file, low_memory=False)
                except pd.errors.ParserError:
                    # If that fails, try tab-delimited
                    df = pd.read_csv(csv_file, sep='\t', low_memory=False, on_bad_lines='skip')
                
                # Clean column names
                df.columns = df.columns.str.strip()
                
                # Convert to records
                records = df.to_dict('records')
                
                # Extract table name from filename (fix: remove .csv extension properly)
                table_name = csv_file.stem.split('.', 1)[-1].replace('.csv', '').replace('_', ' ').title().strip()
                
                # Fix known naming inconsistencies to match mapping file
                table_name_fixes = {
                    'Dad Information': 'DAD Abstract',  # Map to existing section
                    'Dad Diagnosis': 'DAD Diagnosis', 
                    'Dad Interevention': 'DAD Intervention',
                    'Lab Result': 'Laboratory Result',
                    'Admission Discharge': 'Admission/Discharge',  # Remove spaces around slash
                    'Surgery': 'Surgery Case Completed',  # Map to existing section
                    'Previous Admission': 'DAD Special Care Unit',  # Map to closest existing section
                    'Readmission': 'Emergency'  # Map to closest existing section
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

    def load_concept_field_order(self, path: str = "schemas/concept_field_order.json") -> None:
        try:
            with open(path, "r") as f:
                self.concept_field_order = json.load(f)
            logging.info(f"Loaded concept field order for {len(self.concept_field_order)} tables")
        except Exception as e:
            logging.error(f"Failed to load concept_field_order.json: {e}")
            raise

    def load_table_ids(self, path: str = "schemas/table_ids.txt") -> None:
        try:
            with open(path, 'r') as f:
                for line in f:
                    if '=' in line:
                        table, tid = line.strip().split('=')
                        self.table_ids[table.strip()] = int(tid.strip())
            logging.info(f"Loaded table IDs for {len(self.table_ids)} tables")
        except Exception as e:
            logging.error(f"Failed to load table_ids.txt: {e}")
            raise

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
        """Apply a single mapping with optimized record handling and robust type conversion."""
        ihid_field = mapping['ihid_field']
        omop_table = mapping['omop_table']
        omop_field = mapping['omop_field']
        
        # Get the value from source record
        value = source_record.get(ihid_field)
        if value is None or value == '' or (isinstance(value, float) and pd.isna(value)):
            return
        
        # Convert value based on OMOP field requirements and data type
        if omop_field.endswith("_concept_id") and isinstance(value, str):
            concept_id = self._get_or_create_dynamic_concept(omop_table, omop_field, value)
            if concept_id is None:
                return
            converted_value = concept_id
        else:
            converted_value = self._convert_value_robust(value, omop_field, ihid_field, mapping)

        if converted_value is None:
            return
        
        # Generate unique record ID
        record_id = self._generate_record_id(source_record, omop_table)
        
        # Use optimized lookup to find or create record
        if record_id in self.omop_lookup[omop_table]:
            # Update existing record - handle multiple mappings intelligently
            record_index = self.omop_lookup[omop_table][record_id]
            existing_record = self.omop_data[omop_table][record_index]
            
            if omop_field in existing_record and existing_record[omop_field] is not None:
                # Handle multiple values mapping to same field intelligently
                existing_value = existing_record[omop_field]
                combined_value = self._combine_values_intelligently(
                    existing_value, converted_value, omop_field, ihid_field
                )
                existing_record[omop_field] = combined_value
            else:
                # Field doesn't exist yet, just set it
                existing_record[omop_field] = converted_value
        else:
            # Create new record
            new_record = {
                '_record_id': record_id,
                omop_field: converted_value
            }

            # If care_site_name is available, map prefix → concept ID
            # if omop_table == 'care_site' and 'care_site_name' in source_record:
            #     prefix, concept_id = self._get_or_create_place_of_service_concept(source_record['care_site_name'])
            #     if prefix and concept_id:
            #         new_record["place_of_service_source_value"] = prefix
            #         new_record["place_of_service_concept_id"] = concept_id

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
        """Get mappings applicable to this record - field-centric approach."""
        applicable = []
        
        for mapping in self.mapping:
            ihid_field = mapping['ihid_field']
            
            # Field-centric mapping: only check if the field exists in the record
            if ihid_field in source_record:
                applicable.append(mapping)
        
        return applicable
    
    def _convert_value_robust(self, value: Any, omop_field: str, ihid_field: str, mapping: Dict[str, Any]) -> Any:
        """Convert value to appropriate OMOP format with robust type handling."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        
        # Convert based on expected OMOP field type
        field_lower = omop_field.lower()
        ihid_lower = ihid_field.lower()
        
        # ID fields should be integers
        if '_id' in field_lower or field_lower.endswith('_id'):
            try:
                if isinstance(value, str) and value.strip() == '':
                    return None
                return int(float(value))
            except (ValueError, TypeError):
                return None
        
        # Handle datetime fields - multiple sources can map here
        if 'datetime' in field_lower:
            # If it's an elapsed time field, convert differently
            if 'elapsed' in ihid_lower and 'minutes' in ihid_lower:
                return self._convert_elapsed_minutes_to_note(value)
            # Otherwise try to convert as datetime
            return self._convert_to_datetime(value)
        
        # Handle date fields - multiple sources can map here
        if 'date' in field_lower and 'datetime' not in field_lower:
            # If it's an elapsed time field, convert differently
            if 'elapsed' in ihid_lower and 'minutes' in ihid_lower:
                return self._convert_elapsed_minutes_to_note(value)
            # If it's a datetime field being mapped to date, extract date
            if 'dt_tm' in ihid_lower or 'datetime' in ihid_lower:
                datetime_val = self._convert_to_datetime(value)
                if datetime_val:
                    return datetime_val.split(' ')[0]  # Extract date part
                return None
            # Otherwise try to convert as date
            return self._convert_to_date(value)
        
        # Numeric fields - be more specific to avoid false positives
        if ('amount' in field_lower or 'quantity' in field_lower or 
            field_lower.endswith('_value') and not any(x in field_lower for x in ['source_value', 'concept_value'])):
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        # String fields - clean and standardize
        if isinstance(value, str):
            cleaned = value.strip()
            return cleaned if cleaned else None
        
        return value
        
    def _get_or_create_place_of_service_concept(self, care_site_name: str) -> Tuple[Optional[str], Optional[int]]:
        if not care_site_name or not isinstance(care_site_name, str):
            return None, None

        prefix = care_site_name.strip().split(' ')[0].upper()

        if prefix not in self.place_of_service_lookup:
            concept_id = self.next_concept_id
            self.place_of_service_lookup[prefix] = concept_id
            self.next_concept_id += 1

            self.omop_data['concept'].append({
                "concept_id": concept_id,
                "concept_name": prefix,
                "domain_id": "Place of Service",
                "vocabulary_id": "Custom",
                "concept_class_id": "Place of Service",
                "standard_concept": "S",
                "concept_code": prefix,
                "valid_start_date": "1970-01-01",
                "valid_end_date": "2099-12-31",
                "invalid_reason": None
            })

        return prefix, self.place_of_service_lookup[prefix]

    def _get_or_create_dynamic_concept(self, omop_table: str, concept_field: str, raw_value: str) -> Optional[int]:
        """
        Auto-generate concept_id for any *_concept_id field using the XXYYZZ scheme.
        """
        if not raw_value or not isinstance(raw_value, str):
            return None

        value = raw_value.strip().title()

        # Return existing concept_id if already seen
        if value in self.concept_id_counters[omop_table][concept_field]:
            return self.concept_id_counters[omop_table][concept_field][value]

        # Get XX from table_ids.txt
        table_id = self.table_ids.get(omop_table, 99)
        xx = f"{table_id:02d}"

        # Get YY from concept_field_order.json
        field_order = self.concept_field_order.get(omop_table, {}).get(concept_field)
        if field_order is None:
            logging.warning(f"Missing field order for {omop_table}.{concept_field}")
            return None
        yy = f"{field_order:02d}"

        # Get next ZZ for this field
        zz = f"{len(self.concept_id_counters[omop_table][concept_field]):02d}"

        # Construct full concept_id
        concept_id = int(f"{xx}{yy}{zz}")

        # Store and track
        self.concept_id_counters[omop_table][concept_field][value] = concept_id

        # Append to concept table
        self.omop_data["concept"].append({
            "concept_id": concept_id,
            "concept_name": value,
            "domain_id": concept_field.replace("_concept_id", ""),
            "vocabulary_id": "Custom",
            "concept_class_id": "Custom",
            "standard_concept": "S",
            "concept_code": f"{xx}{yy}{zz}",
            "valid_start_date": "1970-01-01",
            "valid_end_date": "2099-12-31",
            "invalid_reason": None
        })

        return concept_id

    def _convert_elapsed_minutes_to_note(self, value: Any) -> Optional[str]:
        """Convert elapsed time in minutes to a descriptive note."""
        try:
            minutes = int(float(value))
            hours = minutes // 60
            remaining_minutes = minutes % 60
            if hours > 0:
                return f"{hours}h {remaining_minutes}m"
            else:
                return f"{minutes}m"
        except (ValueError, TypeError):
            return str(value) if value else None
    
    def _convert_to_datetime(self, value: Any) -> Optional[str]:
        """Convert various datetime formats to OMOP standard (YYYY-MM-DD HH:MM:SS)."""
        if not value or (isinstance(value, float) and pd.isna(value)):
            return None
        
        try:
            # Try to parse with pandas
            datetime_obj = pd.to_datetime(value, errors='coerce')
            if pd.isna(datetime_obj):
                return None
            return datetime_obj.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None
    
    def _combine_values_intelligently(
        self, 
        existing_value: Any, 
        new_value: Any, 
        omop_field: str, 
        ihid_field: str
    ) -> Any:
        """Intelligently combine multiple values for the same OMOP field with robust nesting prevention."""
        
        field_lower = omop_field.lower()
        existing_str = str(existing_value).strip()
        new_str = str(new_value).strip()
        
        # Special handling for day_of_birth - should not combine ages, take first valid one
        if field_lower == 'day_of_birth':
            # Try to convert to age and take the one that makes more sense
            try:
                existing_age = int(float(existing_str.replace(',', '').split()[0]))
                new_age = int(float(new_str.replace(',', '').split()[0]))
                
                # Take the age that's more reasonable (0-120 range)
                if 0 <= existing_age <= 120:
                    return existing_value  # Keep existing if valid
                elif 0 <= new_age <= 120:
                    return new_value  # Replace with new if existing invalid
                else:
                    return existing_value  # Keep existing if both invalid
            except:
                return existing_value  # Keep existing if conversion fails
        
        # Special handling for ID fields - should not combine, take first valid one
        if field_lower.endswith('_id') or field_lower.endswith('_occurrence_id'):
            try:
                # If existing is valid integer, keep it
                int(float(str(existing_value)))
                return existing_value
            except:
                try:
                    # If new is valid integer, use it
                    int(float(str(new_value)))
                    return new_value
                except:
                    return existing_value
        
        # Prevent nested parentheses by checking if already combined
        if "(duration:" in existing_str:
            # Don't nest further, just add with comma if different
            if new_str and new_str not in existing_str:
                return f"{existing_str}, {new_str}"
            return existing_str
        
        # For datetime fields, intelligently combine based on data type
        if 'datetime' in field_lower or 'date' in field_lower:
            # Check if values are datetime format
            is_existing_datetime = self._is_datetime_format(existing_str)
            is_new_datetime = self._is_datetime_format(new_str)
            
            if is_existing_datetime and not is_new_datetime:
                # Existing is datetime, new is duration/other
                return f"{existing_value} (duration: {new_value})"
            elif is_new_datetime and not is_existing_datetime:
                # New is datetime, existing is duration/other
                return f"{new_value} (duration: {existing_value})"
            else:
                # Both same type, combine with comma if different
                if new_str and new_str not in existing_str:
                    return f"{existing_value}, {new_value}"
                return existing_value
        
        # For other fields, combine with commas
        if new_str and new_str not in existing_str:
            return f"{existing_value}, {new_value}"
        
        return existing_value
    
    def _is_datetime_format(self, value_str: str) -> bool:
        """Check if a string represents a datetime format."""
        try:
            # Try to parse as datetime
            pd.to_datetime(value_str, errors='raise')
            # Additional check for common datetime patterns
            return any(pattern in value_str for pattern in ['-', '/', ':', ' ']) and len(value_str) > 8
        except:
            return False
    
    def _convert_value(self, value: Any, omop_field: str, mapping: Dict[str, Any]) -> Any:
        """Convert value to appropriate OMOP format (legacy method for compatibility)."""
        return self._convert_value_robust(value, omop_field, "", mapping)
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
        
        # Numeric fields - be more specific to avoid false positives
        # Only treat as numeric if it's clearly a numeric field, not just contains 'value'
        if ('amount' in field_lower or 'quantity' in field_lower or 
            field_lower.endswith('_value') and not any(x in field_lower for x in ['source_value', 'concept_value'])):
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
            self.load_table_ids()
            self.load_concept_field_order()
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
