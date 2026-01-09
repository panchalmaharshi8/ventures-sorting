#!/usr/bin/env python3
"""
IHID to OMOP ETL Pipeline

Transforms healthcare data from IHID (Integrated Health Information Database) format 
to OMOP Common Data Model format. Supports dynamic concept generation and maintains 
referential integrity across OMOP tables.
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
    """
    IHID to OMOP ETL processor with optimized batch processing and dynamic concept generation.
    
    Transforms healthcare data from IHID format to OMOP Common Data Model format,
    automatically generating OMOP concept IDs using a structured XXYYZZ scheme where:
    - XX: OMOP table ID 
    - YY: Concept field order within table
    - ZZ: Sequential concept number
    """
    def __init__(self, data_dir: str = 'data', mapping_file: str = 'ihid_omop_mapping.json'):
        self.data_dir = Path(data_dir)
        self.mapping_file = mapping_file
        self.ihid_data = {}
        self.omop_data = defaultdict(list)
        self.omop_lookup = defaultdict(dict)
        self.mapping = {}
        self.place_of_service_lookup = {}
        self.next_concept_id = 1000000
        self.concept_id_counters = defaultdict(lambda: defaultdict(dict))
        self.concept_field_order = {}
        self.table_ids = {}
        self.missing_field_order_logged = set()
        
    def load_mapping(self) -> None:
        """Load IHID to OMOP field mappings from JSON configuration file."""
        try:
            with open(self.mapping_file, 'r') as f:
                raw_mapping = json.load(f)
            
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
        """Load and standardize IHID CSV files from the data directory."""
        csv_files = list(self.data_dir.glob('*.csv'))
        
        if not csv_files:
            logging.warning(f"No CSV files found in {self.data_dir}")
            return
        
        total_records = 0
        for csv_file in csv_files:
            try:
                try:
                    df = pd.read_csv(csv_file, low_memory=False)
                except pd.errors.ParserError:
                    df = pd.read_csv(csv_file, sep='\t', low_memory=False, on_bad_lines='skip')
                
                df.columns = df.columns.str.strip()
                records = df.to_dict('records')
                
                table_name = csv_file.stem.split('.', 1)[-1].replace('.csv', '').replace('_', ' ').title().strip()
                
                # Map table names to match mapping configuration
                table_name_fixes = {
                    'Dad Information': 'DAD Abstract',
                    'Dad Diagnosis': 'DAD Diagnosis', 
                    'Dad Interevention': 'DAD Intervention',
                    'Lab Result': 'Laboratory Result',
                    'Admission Discharge': 'Admission/Discharge',
                    'Surgery': 'Surgery Case Completed',
                    'Previous Admission': 'DAD Special Care Unit',
                    'Readmission': 'Emergency'
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

    def load_concept_field_order(self, path: str = None) -> None:
        """Load concept field ordering configuration for dynamic concept ID generation."""
        if path is None:
             # Try to derive from mapping file location
             path = Path(self.mapping_file).parent / "concept_field_order.json"

        try:
            with open(path, "r") as f:
                self.concept_field_order = json.load(f)
            logging.info(f"Loaded concept field order for {len(self.concept_field_order)} tables")
        except Exception as e:
            logging.error(f"Failed to load concept_field_order.json: {e}")
            raise

    def load_table_ids(self, path: str = None) -> None:
        """Load OMOP table ID mappings for concept ID generation."""
        if path is None:
             # Try to derive from mapping file location
             path = Path(self.mapping_file).parent / "table_ids.txt"
        
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
        """Transform loaded IHID data to OMOP format using batch processing for performance."""
        logging.info("Starting IHID to OMOP transformation")
        
        for table_name, records in self.ihid_data.items():
            if not records:
                continue
            
            logging.info(f"Processing {table_name} with {len(records)} records")
            start_time = time.time()
            
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                self._process_batch(table_name, batch)
                
                if len(records) > 10000 and (i + batch_size) % 10000 == 0:
                    elapsed = time.time() - start_time
                    progress = (i + batch_size) / len(records) * 100
                    logging.info(f"  Processed {i + batch_size}/{len(records)} records ({progress:.1f}%) in {elapsed:.1f}s")
            
            elapsed = time.time() - start_time
            logging.info(f"Completed {table_name} in {elapsed:.1f}s")
        
        self._post_process_omop_data()
    
    def _process_batch(self, source_table: str, records: List[Dict[str, Any]]) -> None:
        """Process a batch of IHID records and apply applicable OMOP mappings."""
        for source_record in records:
            source_record = self._standardize_field_names(source_record)
            applicable_mappings = self._get_applicable_mappings(source_table, source_record)
            
            for mapping in applicable_mappings:
                try:
                    self._apply_mapping_optimized(source_record, mapping)
                except Exception as e:
                    logging.debug(f"Error applying mapping {mapping.get('ihid_field')} -> {mapping.get('omop_table')}.{mapping.get('omop_field')}: {e}")
                    continue
    
    def _apply_mapping_optimized(self, source_record: Dict[str, Any], mapping: Dict[str, Any]) -> None:
        """Apply a single IHID->OMOP field mapping with value conversion and record deduplication."""
        ihid_field = mapping['ihid_field']
        omop_table = mapping['omop_table']
        omop_field = mapping['omop_field']
        
        value = source_record.get(ihid_field)
        if value is None or value == '' or (isinstance(value, float) and pd.isna(value)):
            return
        
        # Generate concept ID for concept fields, otherwise convert value type
        if omop_field.endswith("_concept_id") and isinstance(value, str):
            concept_id = self._get_or_create_dynamic_concept(omop_table, omop_field, value)
            if concept_id is None:
                return
            converted_value = concept_id
        else:
            converted_value = self._convert_value_robust(value, omop_field, ihid_field, mapping)

        if converted_value is None:
            return
        
        record_id = self._generate_record_id(source_record, omop_table)
        
        # Update existing record or create new one
        if record_id in self.omop_lookup[omop_table]:
            record_index = self.omop_lookup[omop_table][record_id]
            existing_record = self.omop_data[omop_table][record_index]
            
            if omop_field in existing_record and existing_record[omop_field] is not None:
                existing_value = existing_record[omop_field]
                combined_value = self._combine_values_intelligently(
                    existing_value, converted_value, omop_field, ihid_field
                )
                existing_record[omop_field] = combined_value
            else:
                existing_record[omop_field] = converted_value
        else:
            new_record = {
                '_record_id': record_id,
                omop_field: converted_value
            }
            self._add_standard_identifiers(new_record, source_record, omop_table)
            
            record_index = len(self.omop_data[omop_table])
            self.omop_data[omop_table].append(new_record)
            self.omop_lookup[omop_table][record_id] = record_index
    
    def _standardize_field_names(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Standardize IHID field names to lowercase with underscores for consistent mapping."""
        standardized = {}
        for key, value in record.items():
            clean_key = str(key).lower().strip()
            clean_key = clean_key.replace(' ', '_').replace('-', '_').replace('.', '_')
            while '__' in clean_key:
                clean_key = clean_key.replace('__', '_')
            standardized[clean_key] = value
        return standardized
    
    def _get_applicable_mappings(self, source_table: str, source_record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get mappings applicable to this record based on available IHID fields."""
        applicable = []
        
        for mapping in self.mapping:
            ihid_field = mapping['ihid_field']
            
            if ihid_field in source_record:
                applicable.append(mapping)
        
        return applicable
    
    def _convert_value_robust(self, value: Any, omop_field: str, ihid_field: str, mapping: Dict[str, Any]) -> Any:
        """Convert IHID values to appropriate OMOP data types based on field patterns."""
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        
        field_lower = omop_field.lower()
        ihid_lower = ihid_field.lower()
        
        if '_id' in field_lower or field_lower.endswith('_id'):
            try:
                if isinstance(value, str) and value.strip() == '':
                    return None
                return int(float(value))
            except (ValueError, TypeError):
                return None
        
        if 'datetime' in field_lower:
            if 'elapsed' in ihid_lower and 'minutes' in ihid_lower:
                return self._convert_elapsed_minutes_to_note(value)
            return self._convert_to_datetime(value)
        
        if 'date' in field_lower and 'datetime' not in field_lower:
            if 'elapsed' in ihid_lower and 'minutes' in ihid_lower:
                return self._convert_elapsed_minutes_to_note(value)
            if 'dt_tm' in ihid_lower or 'datetime' in ihid_lower:
                datetime_val = self._convert_to_datetime(value)
                if datetime_val:
                    return datetime_val.split(' ')[0]
                return None
            return self._convert_to_date(value)
        
        if ('amount' in field_lower or 'quantity' in field_lower or 
            field_lower.endswith('_value') and not any(x in field_lower for x in ['source_value', 'concept_value'])):
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        
        if isinstance(value, str):
            cleaned = value.strip()
            return cleaned if cleaned else None
        
        return value

    def _extract_domain_from_concept_field(self, concept_field: str) -> str:
        """Extract domain_id from concept field name for proper OMOP domain classification."""
        if concept_field.endswith("_source_concept_id"):
            return concept_field.replace("_source_concept_id", "")
        
        if concept_field.endswith("_concept_id"):
            return concept_field.replace("_concept_id", "")
        
        if "_concept_id_" in concept_field:
            return concept_field.split("_concept_id_")[0]
        
        return concept_field

    def _get_or_create_dynamic_concept(self, omop_table: str, concept_field: str, raw_value: str) -> Optional[int]:
        """Generate OMOP concept ID using XXYYZZ scheme: XX=table, YY=field, ZZ=sequence."""
        if not raw_value or not isinstance(raw_value, str):
            return None

        value = raw_value.strip().title()

        if value in self.concept_id_counters[omop_table][concept_field]:
            return self.concept_id_counters[omop_table][concept_field][value]

        table_id = self.table_ids.get(omop_table, 99)
        xx = f"{table_id:02d}"

        field_order = self.concept_field_order.get(omop_table, {}).get(concept_field)
        if field_order is None:
            key = f"{omop_table}.{concept_field}"
            if key not in self.missing_field_order_logged:
                logging.warning(f"Missing field order for {key}")
                self.missing_field_order_logged.add(key)
            return None
        yy = f"{field_order:02d}"

        zz = f"{len(self.concept_id_counters[omop_table][concept_field]):02d}"

        concept_id = int(f"{xx}{yy}{zz}")

        self.concept_id_counters[omop_table][concept_field][value] = concept_id

        domain_id = self._extract_domain_from_concept_field(concept_field)

        self.omop_data["concept"].append({
            "concept_id": concept_id,
            "concept_name": value,
            "domain_id": domain_id,
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
        """Convert elapsed time in minutes to human-readable format (e.g., '90' -> '1h 30m')."""
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
        """Combine multiple IHID field values mapping to the same OMOP field, handling data type conflicts."""
        field_lower = omop_field.lower()
        existing_str = str(existing_value).strip()
        new_str = str(new_value).strip()
        
        if field_lower == 'day_of_birth':
            try:
                existing_age = int(float(existing_str.replace(',', '').split()[0]))
                new_age = int(float(new_str.replace(',', '').split()[0]))
                
                if 0 <= existing_age <= 120:
                    return existing_value
                elif 0 <= new_age <= 120:
                    return new_value
                else:
                    return existing_value
            except:
                return existing_value
        
        if field_lower.endswith('_id') or field_lower.endswith('_occurrence_id'):
            try:
                int(float(str(existing_value)))
                return existing_value
            except:
                try:
                    int(float(str(new_value)))
                    return new_value
                except:
                    return existing_value
        
        if "(duration:" in existing_str:
            if new_str and new_str not in existing_str:
                return f"{existing_str}, {new_str}"
            return existing_str
        if 'datetime' in field_lower or 'date' in field_lower:
            is_existing_datetime = self._is_datetime_format(existing_str)
            is_new_datetime = self._is_datetime_format(new_str)
            
            if is_existing_datetime and not is_new_datetime:
                return f"{existing_value} (duration: {new_value})"
            elif is_new_datetime and not is_existing_datetime:
                return f"{new_value} (duration: {existing_value})"
            else:
                if new_str and new_str not in existing_str:
                    return f"{existing_value}, {new_value}"
                return existing_value
        
        if new_str and new_str not in existing_str:
            return f"{existing_value}, {new_value}"
        
        return existing_value
    
    def _is_datetime_format(self, value_str: str) -> bool:
        """Check if a string represents a datetime format."""
        try:
            pd.to_datetime(value_str, errors='raise')
            return any(pattern in value_str for pattern in ['-', '/', ':', ' ']) and len(value_str) > 8
        except:
            return False
    
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
        """Generate unique record identifier for OMOP records based on available source data."""
        mrn = source_record.get('mrn') or source_record.get('medical_record_number')
        encntr_num = source_record.get('encntr_num') or source_record.get('encounter_number')
        event_id = source_record.get('event_id') or source_record.get('clinical_event_id')
        
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
            return f"{omop_table.lower()}_{hash(str(source_record))}"
    
    def _add_standard_identifiers(
        self,
        omop_record: Dict[str, Any],
        source_record: Dict[str, Any],
        omop_table: str
    ) -> None:
        """Add standard OMOP identifiers (person_id, visit_occurrence_id) to records."""
        mrn = source_record.get('mrn') or source_record.get('medical_record_number')
        encntr_num = source_record.get('encntr_num') or source_record.get('encounter_number')
        
        if omop_table.lower() != 'person' and mrn:
            omop_record['person_id'] = mrn
        
        event_tables = [
            'condition_occurrence', 'procedure_occurrence', 'drug_exposure',
            'measurement', 'observation', 'device_exposure', 'specimen'
        ]
        if omop_table.lower() in event_tables and encntr_num:
            omop_record['visit_occurrence_id'] = encntr_num
        
        if omop_table.lower() == 'person' and mrn:
            omop_record['person_id'] = mrn
        elif omop_table.lower() == 'visit_occurrence' and encntr_num:
            omop_record['visit_occurrence_id'] = encntr_num
            if mrn:
                omop_record['person_id'] = mrn
    
    def _generate_domain_table(self) -> None:
        """Generate domain table from ALL concept fields in schema."""
        logging.info("Generating comprehensive domain table from concept field schema")
        
        schema_domains = set()
        for table, fields in self.concept_field_order.items():
            for field_name, order in fields.items():
                if field_name.endswith("_concept_id"):
                    domain_id = field_name.replace("_concept_id", "")
                    schema_domains.add(domain_id)
                elif "_concept_id_" in field_name:
                    parts = field_name.split("_concept_id_")
                    domain_id = parts[0]
                    schema_domains.add(domain_id)
        
        data_domains = set()
        if "concept" in self.omop_data:
            for concept_record in self.omop_data["concept"]:
                domain_id = concept_record.get("domain_id")
                if domain_id and domain_id != "Metadata":
                    data_domains.add(domain_id)
        
        all_domains = schema_domains.union(data_domains)
        
        logging.info(f"Found {len(schema_domains)} domains in schema, {len(data_domains)} domains with data")
        logging.info(f"Generating {len(all_domains)} total domain records")
        
        domain_mapping = {
            # Standard OMOP domains
            'condition': {'name': 'Condition', 'standard': True},
            'procedure': {'name': 'Procedure', 'standard': True},
            'drug': {'name': 'Drug', 'standard': True},
            'measurement': {'name': 'Measurement', 'standard': True},
            'observation': {'name': 'Observation', 'standard': True},
            'device': {'name': 'Device', 'standard': True},
            'visit': {'name': 'Visit', 'standard': True},
            'provider': {'name': 'Provider', 'standard': True},
            'care_site': {'name': 'Care Site', 'standard': True},
            'location': {'name': 'Location', 'standard': True},
            'gender': {'name': 'Gender', 'standard': True},
            'race': {'name': 'Race', 'standard': True},
            'ethnicity': {'name': 'Ethnicity', 'standard': True},
            'relationship': {'name': 'Relationship', 'standard': True},
            'unit': {'name': 'Unit', 'standard': True},
            'currency': {'name': 'Currency', 'standard': True},
            'episode': {'name': 'Episode', 'standard': True},
            'metadata': {'name': 'Metadata', 'standard': True},
            'vocabulary': {'name': 'Vocabulary', 'standard': True},
            
            # Clinical domains
            'cause': {'name': 'Cause of Death', 'standard': False},
            'condition_status': {'name': 'Condition Status', 'standard': False},
            'modifier': {'name': 'Procedure Modifier', 'standard': False},
            'route': {'name': 'Route', 'standard': False},
            'operator': {'name': 'Measurement Operator', 'standard': False},
            'qualifier': {'name': 'Observation Qualifier', 'standard': False},
            'value_as': {'name': 'Value As Concept', 'standard': False},
            
            # Administrative domains
            'admitted_from': {'name': 'Admitted From', 'standard': False},
            'discharged_to': {'name': 'Discharged To', 'standard': False},
            'visit_detail': {'name': 'Visit Detail', 'standard': False},
            'place_of_service': {'name': 'Place of Service', 'standard': False},
            'payer': {'name': 'Payer', 'standard': False},
            'plan': {'name': 'Plan', 'standard': False},
            'sponsor': {'name': 'Sponsor', 'standard': False},
            'stop_reason': {'name': 'Stop Reason', 'standard': False},
            
            # Specimen domains
            'specimen': {'name': 'Specimen', 'standard': False},
            'anatomic_site': {'name': 'Anatomic Site', 'standard': False},
            'disease_status': {'name': 'Disease Status', 'standard': False},
            
            # Drug domains
            'ingredient': {'name': 'Ingredient', 'standard': False},
            'amount_unit': {'name': 'Amount Unit', 'standard': False},
            'numerator_unit': {'name': 'Numerator Unit', 'standard': False},
            'denominator_unit': {'name': 'Denominator Unit', 'standard': False},
            
            # Note domains
            'language': {'name': 'Language', 'standard': False},
            'encoding': {'name': 'Encoding', 'standard': False},
            'note_class': {'name': 'Note Class', 'standard': False},
            'note_event_field': {'name': 'Note Event Field', 'standard': False},
            'note_nlp': {'name': 'Note NLP', 'standard': False},
            'section': {'name': 'Section', 'standard': False},
            
            # Event field domains
            'meas_event_field': {'name': 'Measurement Event Field', 'standard': False},
            'obs_event_field': {'name': 'Observation Event Field', 'standard': False},
            'episode_event_field': {'name': 'Episode Event Field', 'standard': False},
            
            # Cost domains
            'drg': {'name': 'DRG', 'standard': False},
            'revenue_code': {'name': 'Revenue Code', 'standard': False},
            
            # Provider domains
            'specialty': {'name': 'Provider Specialty', 'standard': False},
            
            # Geography domains
            'country': {'name': 'Country', 'standard': False},
            
            # Concept system domains
            'domain': {'name': 'Domain', 'standard': False},
            'concept_class': {'name': 'Concept Class', 'standard': False},
            'ancestor': {'name': 'Ancestor Concept', 'standard': False},
            'descendant': {'name': 'Descendant Concept', 'standard': False},
            'source': {'name': 'Source Concept', 'standard': False},
            'target': {'name': 'Target Concept', 'standard': False},
            
            # Episode domains
            'episode_object': {'name': 'Episode Object', 'standard': False},
            'subject': {'name': 'Cohort Subject', 'standard': False},
            
            # Version domains
            'cdm_version': {'name': 'CDM Version', 'standard': False}
        }
        
        domain_table_id = self.table_ids.get("domain", 30)
        
        domain_records = []
        domain_concept_counter = 0
        
        for domain_id in sorted(all_domains):
            if domain_id == "Metadata":
                continue
                
            domain_info = domain_mapping.get(domain_id, {
                'name': domain_id.replace('_', ' ').title(), 
                'standard': False
            })
            
            domain_concept_id = int(f"{domain_table_id:02d}01{domain_concept_counter:02d}")
            
            domain_record = {
                "domain_id": domain_id,
                "domain_name": domain_info['name'],
                "domain_concept_id": domain_concept_id
            }
            
            domain_records.append(domain_record)
            
            domain_concept = {
                "concept_id": domain_concept_id,
                "concept_name": domain_info['name'],
                "domain_id": "Metadata",
                "vocabulary_id": "Domain",
                "concept_class_id": "Domain",
                "standard_concept": "S" if domain_info['standard'] else "C",
                "concept_code": domain_id.upper(),
                "valid_start_date": "1970-01-01",
                "valid_end_date": "2099-12-31",
                "invalid_reason": None
            }
            
            self.omop_data["concept"].append(domain_concept)
            domain_concept_counter += 1
        
        self.omop_data["domain"] = domain_records
        
        logging.info(f"Generated {len(domain_records)} comprehensive domain records")
        if len(domain_records) <= 20:  # Only log all if reasonable number
            for record in domain_records:
                logging.info(f"  {record['domain_id']}: {record['domain_name']} (concept_id: {record['domain_concept_id']})")
        else:
            for record in domain_records[:5]:
                logging.info(f"  {record['domain_id']}: {record['domain_name']} (concept_id: {record['domain_concept_id']})")
            logging.info(f"  ... and {len(domain_records) - 5} more domain records")
    
    def _post_process_omop_data(self) -> None:
        """Post-process OMOP data to ensure consistency and add required fields."""
        logging.info("Post-processing OMOP data")
        
        self._generate_domain_table()
        
        for table_name, records in self.omop_data.items():
            for record in records:
                record.pop('_record_id', None)
        
        self.omop_lookup.clear()
        
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
            
            self.load_mapping()
            self.load_table_ids()
            self.load_concept_field_order()
            self.load_csv_data()
            
            if not self.ihid_data:
                logging.error("No IHID data loaded. Exiting.")
                return
            
            self.transform_to_omop()
            
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
