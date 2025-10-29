#!/usr/bin/env python3
"""
Enhanced IHID → FHIR Mapper

Parses schemas/FIHR_Summarized_Schema.xlsx and produces a mapping JSON
that maps IHID fields to FHIR resource fields. The output mirrors the
shape used by the OMOP mapper but uses keys: fhir_resource, fhir_field,
and mapping_type (exact|non-exact).
"""

import json
import os
import logging
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple, Union

import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class EnhancedIHIDFHIRMapper:
    def __init__(
        self,
        ihid_catalog_path: str,
        fhir_schema_path: str,
        output_mapping_path: str
    ):
        self.ihid_catalog_path = ihid_catalog_path
        self.fhir_schema_path = fhir_schema_path
        self.output_mapping_path = output_mapping_path

        self.ihid_catalog: Dict[str, List[Dict[str, Any]]] = {}
        self.mapping: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))

        self._load_ihid_catalog()
        self._load_and_process_fhir_schema()

    def _load_ihid_catalog(self) -> None:
        logging.info(f"Loading IHID catalog from {self.ihid_catalog_path}")
        with open(self.ihid_catalog_path, 'r', encoding='utf-8') as f:
            rows = json.load(f)

        catalog = defaultdict(list)
        for row in rows:
            table = row.get('Source_Section')
            col = row.get('Column Name')
            if table and col:
                catalog[table].append({
                    'name': col,
                    'type': row.get('Data Type'),
                    'explanation': row.get('Explanation')
                })
        self.ihid_catalog = catalog
        logging.info(f"Loaded {len(self.ihid_catalog)} IHID tables with {sum(len(cols) for cols in self.ihid_catalog.values())} total columns")

    def _load_and_process_fhir_schema(self) -> None:
        logging.info(f"Loading FHIR summarized schema from {self.fhir_schema_path}")
        xls = pd.ExcelFile(self.fhir_schema_path)

        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name)
            self._process_sheet(str(sheet_name), df)

    def _process_sheet(self, sheet_name: str, df: pd.DataFrame) -> None:
        # Normalize column names we saw across sheets
        cols = {c: c for c in df.columns}
        # Handle minor typos/variants: IHIID vs IHID, alternatives column only on some sheets
        col_subcat = self._first_existing(df, ['Subcategory'])
        col_table = self._first_existing(df, ['Table'])
        col_field = self._first_existing(df, ['FHIR Field'])
        col_equiv = self._first_existing(df, ['IHID Equivalent', 'IHIID Equivalent'])
        col_notes = self._first_existing(df, ['Notes', 'IHID Alternatives'])

        if not col_field or not col_table:
            logging.warning(f"Skipping sheet '{sheet_name}' due to missing columns")
            return

        current_table: Optional[str] = None  # FHIR table/resource from 'Table' column
        current_resource_header: Optional[str] = None  # Resource header from 'FHIR Field' column when it shows only resource name
        current_subcategory: Optional[str] = None  # Track Subcategory header; blank rows inherit previous

        for _, row in df.iterrows():
            table_val = self._safe_str(row.get(col_table))
            field_val = self._safe_str(row.get(col_field))
            equiv_val = row.get(col_equiv)
            notes_val = row.get(col_notes)
            header_val = self._safe_str(row.get(col_subcat))

            # Track subcategory as soon as we see it so it applies to both resource header rows and field rows
            if header_val:
                current_subcategory = header_val

            # Track current table; blank means continue with previous
            if table_val:
                current_table = table_val

            # Identify resource header vs field rows
            if field_val and '.' not in field_val:
                current_resource_header = field_val.strip()
                continue

            if not current_resource_header or not field_val:
                continue

            # Parse FHIR path like 'Patient.identifier.use'
            resource, path = self._split_resource_path(current_resource_header, field_val)
            if not resource or not path:
                continue

            # Prefer explicit table name if provided; otherwise use resource header
            fhir_table = current_table or resource
            fhir_resource = resource

            # Gather IHID mappings
            exact_fields = self._parse_field_list(equiv_val)
            alt_fields = self._parse_field_list(notes_val)

            # Prefer exacts as 'exact', alternatives as 'non-exact' when exacts missing
            if exact_fields:
                for src in exact_fields:
                    stbl, sfld = self._parse_source_field(src)
                    if stbl and sfld:
                        self._add_mapping(stbl, sfld, fhir_resource, path, 'exact', sheet_name, current_subcategory or 'General', fhir_table=fhir_table)
            if alt_fields:
                for src in alt_fields:
                    stbl, sfld = self._parse_source_field(src)
                    if stbl and sfld:
                        # Only add non-exact when an exact wasn't also specified for same field
                        self._add_mapping(stbl, sfld, fhir_resource, path, 'non-exact', sheet_name, current_subcategory or 'General', fhir_table=fhir_table)

    def _first_existing(self, df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        for c in candidates:
            if c in df.columns:
                return c
        return None

    def _safe_str(self, v: Any) -> str:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ''
        return str(v).strip()

    def _parse_field_list(self, value: Union[str, float, None]) -> List[str]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return []
        if isinstance(value, str):
            out: List[str] = []
            for line in value.split('\n'):
                for part in line.split(','):
                    s = part.strip()
                    if s:
                        out.append(s)
            return out
        return []

    def _split_resource_path(self, resource_header: str, field_path: str) -> Tuple[Optional[str], Optional[str]]:
        # field_path expected like 'Patient.identifier' while resource_header is 'Patient'
        if '.' in field_path:
            parts = field_path.split('.', 1)
            res = parts[0].strip()
            path = parts[1].strip()
        else:
            res = resource_header.strip()
            path = field_path.strip()
        if not res or not path:
            return None, None
        return res, path

    def _parse_source_field(self, field_with_source: str) -> Tuple[Optional[str], Optional[str]]:
        # Expected like 'Admission.gender_desc_at_admit' or with abbreviations
        if '.' in field_with_source:
            parts = field_with_source.split('.', 1)
            source_table = parts[0].strip()
            field_name = parts[1].strip()

            source_mapping = {
                'Admission': 'Admission / Discharge',
                'AdDis': 'Admission / Discharge',
                'DADAbs': 'DAD Information',
                'DAD Information': 'DAD Information',
                'DADDx': 'DAD Diagnosis',
                'DADDiag': 'DAD Diagnosis',
                'DADInt': 'DAD Intervention',
                'ClinEv': 'Clinical Event',
                'Lab': 'Laboratory Result',
                'Surg': 'Surgery Case Completed',
                'Surgery': 'Surgery Case Completed',
                'Readm': 'Readmission',
                'PrevAdm': 'Previous Admission',
                'Emerg': 'Emergency',
                'MedIm': 'Medical Imaging',  # may not exist in catalog
                'Cens': 'Census',            # may not exist in catalog
                'Active Medical Service': 'Active Medical Service',
                'ActMedServ': 'Active Medical Service',
                'Ord': 'Orders',            # may not exist in catalog
                'DIM': 'Diagnostic Imaging' # best effort
            }

            mapped_source: Optional[str] = None
            for abbrev, full_name in source_mapping.items():
                if source_table == abbrev or source_table.startswith(abbrev):
                    mapped_source = full_name
                    break

            if not mapped_source:
                for catalog_table in self.ihid_catalog.keys():
                    if source_table.lower() in catalog_table.lower():
                        mapped_source = catalog_table
                        break

            return mapped_source or source_table, field_name
        else:
            return None, field_with_source

    def _add_mapping(
        self,
        ihid_table: str,
        ihid_field: str,
        fhir_resource: str,
        fhir_field: str,
        mapping_type: str,
        category: Optional[str] = None,
        header: Optional[str] = None,
        fhir_table: Optional[str] = None
    ) -> bool:
        # Validate IHID table presence (best-effort match)
        if ihid_table not in self.ihid_catalog:
            for catalog_table in self.ihid_catalog.keys():
                if ihid_table.lower() in catalog_table.lower() or catalog_table.lower() in ihid_table.lower():
                    logging.info(f"Mapping table '{ihid_table}' to '{catalog_table}'")
                    ihid_table = catalog_table
                    break
            else:
                logging.info(f"IHID table '{ihid_table}' not found in catalog; keeping mapping (non-strict)")

        entry = {
            'fhir_resource': fhir_resource,
            'fhir_table': fhir_table or fhir_resource,
            'fhir_field': fhir_field,
            'mapping_type': mapping_type,
            'category': category,
            'header': header
        }
        self.mapping[ihid_table][ihid_field].append(entry)
        return True

    def save_mapping(self) -> None:
        logging.info(f"Saving FHIR mapping to {self.output_mapping_path}")
        mapping_dict: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        for table, fields in self.mapping.items():
            mapping_dict[table] = {}
            for field, entries in fields.items():
                mapping_dict[table][field] = entries
        with open(self.output_mapping_path, 'w', encoding='utf-8') as f:
            json.dump(mapping_dict, f, indent=2, ensure_ascii=False)
        logging.info("FHIR mapping saved successfully")


def main():
    ihid_catalog_path = 'schemas/All_Tables_Combined.json'
    fhir_schema_path = 'schemas/FIHR_Summarized_Schema.xlsx'
    output_mapping_path = 'schemas/ihid_fhir_mapping.json'

    for fp in [ihid_catalog_path, fhir_schema_path]:
        if not os.path.exists(fp):
            logging.error(f"Required file not found: {fp}")
            return 1

    mapper = EnhancedIHIDFHIRMapper(
        ihid_catalog_path=ihid_catalog_path,
        fhir_schema_path=fhir_schema_path,
        output_mapping_path=output_mapping_path
    )
    mapper.save_mapping()
    logging.info("Enhanced IHID → FHIR mapping process completed successfully")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
