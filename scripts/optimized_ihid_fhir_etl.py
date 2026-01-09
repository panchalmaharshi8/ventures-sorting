#!/usr/bin/env python3
"""
IHID to FHIR ETL Pipeline

Transforms IHID data into simplified FHIR-like JSON resources using a mapping
generated from FIHR_Summarized_Schema.xlsx.
"""

import json
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, cast

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class OptimizedIHIDToFHIRETL:
    def __init__(self, data_dir: str = 'data', mapping_file: str = 'schemas/ihid_fhir_mapping.json'):
        self.data_dir = Path(data_dir)
        self.mapping_file = mapping_file
        self.ihid_data: Dict[str, List[Dict[str, Any]]] = {}
        self.mapping: List[Dict[str, Any]] = []

        # Keyed by (category, header, table)
        self.fhir_data: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
        self.fhir_lookup: Dict[Tuple[str, str, str], Dict[str, int]] = defaultdict(dict)

        # Full index of all (category, header, table) combos from schema
        self.fhir_index: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))

        # Table name canonicalization: map various IHID table labels to a common key
        # This is critical because the mapping may use different display names than the CSV loader.
        self._table_aliases = {
            # DAD
            'dad information': 'dad_information',
            'dad abstract': 'dad_information',
            'dad diagnosis': 'dad_diagnosis',
            'dad intervention': 'dad_intervention',
            'dad interevention': 'dad_intervention',  # common typo
            'dad special care unit': 'dad_special_care_unit',
            # Encounters / ED
            'admission/discharge': 'admission_discharge',
            'admission discharge': 'admission_discharge',
            'emergency': 'emergency',
            # Clinical
            'clinical event': 'clinical_event',
            # Surgery
            'surgery': 'surgery',
            'surgery case completed': 'surgery',
            # Lab
            'laboratory result': 'laboratory_result',
            'lab result': 'laboratory_result',
            'lab results': 'laboratory_result',
            # Others referenced in mapping (may be absent in CSV set)
            'orders': 'orders',
            'medical imaging': 'medical_imaging',
            'active medical service': 'active_medical_service',
        }

    def _canonical_table(self, name: str) -> str:
        """Return a canonical key for a table name to align mapping and CSV loader labels."""
        key = str(name).strip().lower()
        # Normalize whitespace and punctuation
        key = key.replace('\t', ' ').replace('\n', ' ').replace('_', ' ')
        while '  ' in key:
            key = key.replace('  ', ' ')
        key = key.strip()
        # Remove leading numeric prefixes like "1. "
        if len(key) > 2 and key[0].isdigit():
            # split on first space after a dot if present
            parts = key.split(' ', 1)
            if parts and parts[0].rstrip('.').isdigit() and len(parts) > 1:
                key = parts[1]
        # Title-style variants to a consistent token
        key = key.replace('\\', '/').replace('-', ' ')
        while '  ' in key:
            key = key.replace('  ', ' ')
        key = key.strip()
        return self._table_aliases.get(key, key.replace(' ', '_'))

    def load_mapping(self) -> None:
        try:
            with open(self.mapping_file, 'r', encoding='utf-8') as f:
                raw_mapping = json.load(f)
            self.mapping = []
            count = 0
            for source_table, field_mappings in raw_mapping.items():
                for ihid_field, target_list in field_mappings.items():
                    for entry in target_list:
                        if isinstance(entry, dict):
                            self.mapping.append({
                                'source_table': source_table,
                                'ihid_field': ihid_field,
                                'fhir_resource': entry.get('fhir_resource'),
                                'fhir_table': entry.get('fhir_table') or entry.get('fhir_resource'),
                                'fhir_field': entry.get('fhir_field'),
                                'mapping_type': entry.get('mapping_type'),
                                'category': entry.get('category') or 'Uncategorized',
                                'header': entry.get('header') or 'General'
                            })
                            count += 1
            logging.info(f"Loaded {count} FHIR mappings from {self.mapping_file}")
        except Exception as e:
            logging.error(f"Failed to load FHIR mapping: {e}")
            raise

    def load_csv_data(self) -> None:
        csv_files = list(self.data_dir.glob('*.csv'))
        if not csv_files:
            logging.warning(f"No CSV files found in {self.data_dir}")
            return
        total = 0
        name_fixes = {
            'Dad Information': 'DAD Abstract',
            'Dad Diagnosis': 'DAD Diagnosis',
            'Dad Interevention': 'DAD Intervention',
            'Lab Result': 'Laboratory Result',
            'Admission Discharge': 'Admission/Discharge',
            'Surgery': 'Surgery Case Completed',
            'Previous Admission': 'DAD Special Care Unit',
            'Readmission': 'Emergency'
        }
        for csv in csv_files:
            try:
                try:
                    df = pd.read_csv(csv, low_memory=False)
                except pd.errors.ParserError:
                    df = pd.read_csv(csv, sep='\t', low_memory=False, on_bad_lines='skip')
                df.columns = df.columns.str.strip()
                records = df.to_dict('records')
                table_name = csv.stem.split('.', 1)[-1].replace('.csv', '').replace('_', ' ').title().strip()
                if table_name in name_fixes:
                    table_name = name_fixes[table_name]
                # Keep original display name for logging, but also store under canonical key for easier matching
                self.ihid_data[table_name] = cast(List[Dict[str, Any]], records)
                canonical = self._canonical_table(table_name)
                if canonical not in self.ihid_data:
                    self.ihid_data[canonical] = cast(List[Dict[str, Any]], records)
                total += len(records)
                logging.info(f"Loaded {len(records)} records from {csv.name} as {table_name}")
            except Exception as e:
                logging.error(f"Error loading {csv}: {e}")
        logging.info(f"Loaded {len(self.ihid_data)} CSV tables with {total} total records")

    def transform_to_fhir(self) -> None:
        logging.info("Starting IHID to FHIR transformation")
        for table_name, records in self.ihid_data.items():
            if not records:
                continue
            start = time.time()
            batch_size = 1000
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                self._process_batch(table_name, batch)
                if len(records) > 10000 and (i + batch_size) % 10000 == 0:
                    elapsed = time.time() - start
                    progress = (i + batch_size) / len(records) * 100
                    logging.info(f"  Processed {i + batch_size}/{len(records)} records ({progress:.1f}%) in {elapsed:.1f}s")
            elapsed = time.time() - start
            logging.info(f"Completed {table_name} in {elapsed:.1f}s")

    def _process_batch(self, source_table: str, records: List[Dict[str, Any]]) -> None:
        for record in records:
            std = self._standardize_field_names(record)
            mappings = self._get_applicable_mappings(source_table, std)
            for mapping in mappings:
                try:
                    self._apply_mapping(std, mapping)
                except Exception as e:
                    logging.debug(f"Error applying mapping {mapping.get('ihid_field')} -> {mapping.get('fhir_resource')}.{mapping.get('fhir_field')}: {e}")

    def _apply_mapping(self, source_record: Dict[str, Any], mapping: Dict[str, Any]) -> None:
        ihid_field = mapping.get('_ihid_field_std') or self._standardize_field_key(mapping['ihid_field'])
        resource = mapping['fhir_resource']
        fhir_table = mapping['fhir_table']
        field_path = mapping['fhir_field']
        category = mapping.get('category', 'Uncategorized')
        header = mapping.get('header', 'General')
        value = source_record.get(ihid_field)
        if value is None or value == '' or (isinstance(value, float) and pd.isna(value)):
            return

        converted = self._convert_value_robust(value, field_path, ihid_field, mapping)
        if converted is None:
            return

        resource_id = self._generate_record_id(source_record, resource)
        key = (category, header, fhir_table)
        if resource_id in self.fhir_lookup[key]:
            idx = self.fhir_lookup[key][resource_id]
            res_obj = self.fhir_data[key][idx]
        else:
            res_obj = {'resourceType': resource, 'id': resource_id}
            idx = len(self.fhir_data[key])
            self.fhir_data[key].append(res_obj)
            self.fhir_lookup[key][resource_id] = idx

        # combine values thoughtfully: if path already set, append or merge
        existing_val = self._get_by_fhir_path(res_obj, field_path)
        if existing_val is not None:
            combined = self._combine_values_intelligently(existing_val, converted, field_path, ihid_field)
            self._set_by_fhir_path(res_obj, field_path, combined)
        else:
            self._set_by_fhir_path(res_obj, field_path, converted)

    def _standardize_field_names(self, record: Dict[str, Any]) -> Dict[str, Any]:
        out = {}
        for k, v in record.items():
            ck = str(k).lower().strip().replace(' ', '_').replace('-', '_').replace('.', '_')
            while '__' in ck:
                ck = ck.replace('__', '_')
            out[ck] = v
        return out

    def _standardize_field_key(self, key: str) -> str:
        """Standardize a single field name to match _standardize_field_names keys."""
        ck = str(key).lower().strip().replace(' ', '_').replace('-', '_').replace('.', '_')
        while '__' in ck:
            ck = ck.replace('__', '_')
        return ck

    def _get_applicable_mappings(self, source_table: str, source_record: Dict[str, Any]) -> List[Dict[str, Any]]:
        applicable: List[Dict[str, Any]] = []
        src_canon = self._canonical_table(source_table)
        for m in self.mapping:
            # Match mapping source table to canonical form of current CSV table
            map_src_canon = self._canonical_table(m['source_table'])
            if map_src_canon == src_canon:
                ihid_key_std = self._standardize_field_key(m['ihid_field'])
                if ihid_key_std in source_record:
                    m.setdefault('_ihid_field_std', ihid_key_std)
                    applicable.append(m)
        return applicable

    def _convert_value_robust(self, value: Any, fhir_field: str, ihid_field: str, mapping: Dict[str, Any]) -> Any:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        field_lower = fhir_field.lower()
        ihid_lower = ihid_field.lower()

        if 'date' in field_lower or 'datetime' in field_lower or field_lower.endswith('time'):
            if 'elapsed' in ihid_lower and 'minutes' in ihid_lower:
                return self._convert_elapsed_minutes_to_note(value)
            # Use pandas for permissive parsing
            try:
                dt = pd.to_datetime(value, errors='coerce')
                if pd.isna(dt):
                    return None
                # if field suggests date only
                if 'date' in field_lower and 'time' not in field_lower and 'datetime' not in field_lower:
                    return dt.strftime('%Y-%m-%d')
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                return None

        # numeric-ish
        if any(x in field_lower for x in ['value', 'quantity', 'amount', 'numerator', 'denominator']):
            try:
                return float(value)
            except (ValueError, TypeError):
                pass

        if isinstance(value, str):
            s = value.strip()
            return s if s else None
        return value

    def _convert_elapsed_minutes_to_note(self, value: Any) -> Optional[str]:
        try:
            minutes = int(float(value))
            h = minutes // 60
            m = minutes % 60
            return f"{h}h {m}m" if h > 0 else f"{minutes}m"
        except (ValueError, TypeError):
            return str(value) if value else None

    def _combine_values_intelligently(self, existing_value: Any, new_value: Any, fhir_field: str, ihid_field: str) -> Any:
        exs = str(existing_value).strip()
        news = str(new_value).strip()
        if news and news not in exs:
            return f"{existing_value}, {new_value}"
        return existing_value

    def _set_by_fhir_path(self, obj: Dict[str, Any], path: str, value: Any) -> None:
        parts = path.split('.') if path else []
        cur = obj
        for i, part in enumerate(parts):
            is_last = i == len(parts) - 1
            is_list = part.endswith('[]')
            key = part[:-2] if is_list else part
            if is_last:
                if is_list:
                    cur.setdefault(key, [])
                    cur[key].append(value)
                else:
                    cur[key] = value
            else:
                if key not in cur or not isinstance(cur[key], dict):
                    cur[key] = {}
                cur = cur[key]

    def _get_by_fhir_path(self, obj: Dict[str, Any], path: str) -> Any:
        parts = path.split('.') if path else []
        cur: Any = obj
        for i, part in enumerate(parts):
            is_last = i == len(parts) - 1
            is_list = part.endswith('[]')
            key = part[:-2] if is_list else part
            if key not in cur:
                return None
            cur = cur[key]
            if is_last:
                return cur
            if not isinstance(cur, dict):
                return None
        return None

    def _generate_record_id(self, source_record: Dict[str, Any], resource: str) -> str:
        mrn = source_record.get('mrn') or source_record.get('medical_record_number')
        encntr = source_record.get('encntr_num') or source_record.get('encounter_number')
        event_id = source_record.get('event_id') or source_record.get('clinical_event_id')
        if resource.lower() == 'patient':
            return f"patient-{mrn}" if mrn else f"patient-unknown-{hash(str(source_record))}"
        if resource.lower() in ['encounter', 'procedure', 'condition', 'observation', 'medicationstatement', 'medicationrequest']:
            base = f"{mrn}-{encntr}" if mrn and encntr else str(hash(str(source_record)))
            return f"{resource.lower()}-{base}"
        if event_id:
            return f"{resource.lower()}-{event_id}"
        return f"{resource.lower()}-{hash(str(source_record))}"

    def save_fhir_data(self, output_dir: str = 'fhir_output') -> None:
        out = Path(output_dir)
        out.mkdir(exist_ok=True)
        logging.info(f"Saving FHIR resources to {output_dir}")

        # Ensure every (category/header/table) produces a file, even if empty
        for category, headers in self.fhir_index.items():
            for header, tables in headers.items():
                for table in tables:
                    key = (category, header, table)
                    items = self.fhir_data.get(key, [])
                    cat_dir = out / self._safe_dir(category)
                    header_dir = cat_dir / self._safe_dir(header)
                    header_dir.mkdir(parents=True, exist_ok=True)
                    fp = header_dir / f"{table}.json"
                    try:
                        with open(fp, 'w', encoding='utf-8') as f:
                            json.dump(items, f, indent=2, ensure_ascii=False, default=str)
                        logging.info(f"Saved {len(items)} {table} resources to {fp}")
                    except Exception as e:
                        logging.error(f"Error saving {fp}: {e}")

    def _safe_dir(self, name: str) -> str:
        s = name.strip().replace('/', '-').replace('\\', '-')
        return s

    def load_fhir_index(self, schema_path: str = None) -> None:
        if schema_path is None:
            # Try to derive from mapping file's directory
            schema_path = str(Path(self.mapping_file).parent / 'FIHR_Summarized_Schema.xlsx')
            
        try:
            xls = pd.ExcelFile(schema_path)
            for sheet in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet)
                col_table = None
                for c in ['Table']:
                    if c in df.columns:
                        col_table = c
                        break
                col_header = None
                for c in ['Subcategory']:
                    if c in df.columns:
                        col_header = c
                        break
                if not col_table or not col_header:
                    continue
                current_table: Optional[str] = None
                current_header: Optional[str] = None
                for _, row in df.iterrows():
                    table_val = row.get(col_table)
                    header_val = row.get(col_header)
                    if isinstance(header_val, str) and header_val.strip():
                        current_header = header_val.strip()
                    if isinstance(table_val, str) and table_val.strip():
                        current_table = table_val.strip()
                    if current_table and current_header:
                        category = str(sheet).strip()
                        self.fhir_index[category][current_header].add(current_table)
        except Exception as e:
            logging.warning(f"Failed to load FHIR index: {e}")

    def run_etl(self) -> None:
        start = time.time()
        logging.info("Starting IHID to FHIR ETL pipeline")
        self.load_mapping()
        self.load_fhir_index()
        self.load_csv_data()
        if not self.ihid_data:
            logging.error("No IHID data loaded. Exiting.")
            return
        self.transform_to_fhir()
        self.save_fhir_data()
        elapsed = time.time() - start
        logging.info(f"FHIR ETL pipeline completed successfully in {elapsed:.1f} seconds")


def main():
    etl = OptimizedIHIDToFHIRETL()
    etl.run_etl()


if __name__ == '__main__':
    main()
