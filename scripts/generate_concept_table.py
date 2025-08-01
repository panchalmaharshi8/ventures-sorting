#!/usr/bin/env python3

import json
import pandas as pd
from pathlib import Path
from typing import Dict, List, Set, Tuple
from collections import defaultdict

class OMOPConceptGenerator:
    def __init__(self):
        self.table_ids = {}
        self.concept_fields = []
        self.concept_records = []
        self.table_field_counters = defaultdict(int)  # Track field order within each table
        self.schema_path = Path("schemas")
        self.output_path = Path("omop_output")
        
    def load_table_ids(self) -> None:
        """Load table ID mappings from table_ids.txt."""
        table_ids_path = self.schema_path / "table_ids.txt"
        
        if not table_ids_path.exists():
            raise FileNotFoundError(f"Table IDs file not found: {table_ids_path}")
        
        with open(table_ids_path, 'r') as f:
            for line in f:
                line = line.strip()
                if '=' in line:
                    table_name, table_id = line.split('=', 1)
                    self.table_ids[table_name.strip()] = int(table_id.strip())
        
        print(f"Loaded {len(self.table_ids)} table IDs")
        
    def load_concept_schema(self) -> List[str]:
        """Load concept table fields from OMOP schema."""
        schema_path = self.schema_path / "OMOP_Summarized_Schema.xlsx"
        
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        # Read Excel file
        df = pd.read_excel(schema_path)
        
        # Filter for concept table rows
        concept_rows = df[df['table_name'] == 'concept']
        concept_fields = concept_rows['field_name'].tolist()
        
        print(f"Found {len(concept_fields)} concept table fields: {concept_fields}")
        return concept_fields
        
    def extract_concept_id_fields(self) -> None:
        """Extract all concept_id fields from OMOP schema Excel file."""
        schema_path = self.schema_path / "OMOP_Summarized_Schema.xlsx"
        
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        # Read Excel file
        df = pd.read_excel(schema_path)
        
        # Filter for fields containing 'concept_id' (case insensitive)
        concept_id_rows = df[df['field_name'].str.contains('concept_id', case=False, na=False)]
        
        concept_fields = []
        
        for _, row in concept_id_rows.iterrows():
            table_name = row['table_name']
            field_name = row['field_name']
            description = row.get('description', '')
            
            # Skip the concept table itself - we're generating that
            if table_name.lower() != 'concept':
                concept_fields.append({
                    'omop_table': table_name,
                    'omop_field': field_name,
                    'description': description,
                    'source_table': 'Excel Schema',  # Placeholder
                    'ihid_field': 'N/A',  # Placeholder
                    'mapping_type': 'concept_id'
                })
        
        self.concept_fields = concept_fields
        print(f"Found {len(concept_fields)} concept_id fields in Excel schema")
        
        # Print all found concept_id fields for verification
        print("Concept ID fields found:")
        for field in concept_fields:
            print(f"  {field['omop_table']}.{field['omop_field']}")
        
    def generate_concept_id(self, omop_table: str, field_order: int) -> str:
        """Generate concept ID in format XXYYZZ where XX=table ID, YY=field order, ZZ=xx placeholder."""
        # Get table ID (XX)
        table_id = self.table_ids.get(omop_table, 99)  # Default to 99 if not found
        xx = f"{table_id:02d}"
        
        # YY is the order of concept_id field within this table
        yy = f"{field_order:02d}"
        
        # ZZ is placeholder 'xx' for now (will be filled with real data later)
        zz = "xx"
        
        return f"{xx}{yy}{zz}"
        
    def extract_domain_name(self, omop_field: str) -> str:
        """Extract domain name from concept_id field name by removing _concept_id suffix."""
        # Simply remove '_concept_id' from the field name to get domain name
        if omop_field.endswith('_concept_id'):
            return omop_field.replace('_concept_id', '')
        else:
            # Fallback for fields that don't end with _concept_id
            return omop_field
        
    def create_concept_records(self, concept_schema: List[str]) -> None:
        """Create concept records for all concept_id fields."""
        # Group concept fields by table to maintain order within each table
        table_fields = defaultdict(list)
        
        # Group fields by table first
        for field_info in self.concept_fields:
            omop_table = field_info['omop_table']
            table_fields[omop_table].append(field_info)
        
        # Process each table and assign field order numbers
        for omop_table, fields in table_fields.items():
            field_order = 0
            for field_info in fields:
                field_order += 1
                self.table_field_counters[omop_table] = field_order
                
                omop_field = field_info['omop_field']
                
                # Extract domain name from the field name
                domain_id = self.extract_domain_name(omop_field)
                
                # Generate concept ID with field order
                concept_id = self.generate_concept_id(omop_table, field_order)
                
                # Create concept record with all fields
                concept_record = {}
                
                # Initialize all concept fields as None
                for field in concept_schema:
                    concept_record[field] = None
                
                # Populate required fields
                concept_record['concept_id'] = concept_id  # Keep as string since it contains 'xx'
                concept_record['domain_id'] = domain_id
                
                # Add basic fields - keeping classification fields empty for now
                # Leave concept_name blank for now
                concept_record['concept_name'] = ""
                concept_record['concept_code'] = None  # To be defined later
                concept_record['vocabulary_id'] = None  # To be defined later
                concept_record['concept_class_id'] = None  # To be defined later
                concept_record['standard_concept'] = "S"
                concept_record['valid_start_date'] = "1970-01-01"
                concept_record['valid_end_date'] = "2099-12-31"
                concept_record['invalid_reason'] = None
                
                self.concept_records.append(concept_record)
                
                print(f"Created concept {concept_id} for {omop_table}.{omop_field} (order {field_order})")
        
        print(f"Generated {len(self.concept_records)} concept records")
        
    def save_concept_table(self) -> None:
        """Save the concept table to JSON file."""
        self.output_path.mkdir(exist_ok=True)
        
        output_file = self.output_path / "concept.json"
        
        with open(output_file, 'w') as f:
            json.dump(self.concept_records, f, indent=2, default=str)
        
        print(f"Saved {len(self.concept_records)} concept records to {output_file}")
        
    def print_summary(self) -> None:
        """Print summary of generated concepts."""
        print(f"\n=== CONCEPT GENERATION SUMMARY ===")
        print(f"Total concept records: {len(self.concept_records)}")
        print(f"Tables with concept_id fields: {len(self.table_field_counters)}")
        
        print(f"\nTable breakdown:")
        for table_name, field_count in sorted(self.table_field_counters.items()):
            table_id = self.table_ids.get(table_name, 99)
            print(f"  {table_name} (ID {table_id:02d}): {field_count} concept_id fields")
            
        print(f"\nConcept ID format examples:")
        table_examples = defaultdict(list)
        for record in self.concept_records:
            concept_id = str(record['concept_id'])
            table_id = concept_id[:2]
            table_name = None
            for name, tid in self.table_ids.items():
                if f"{tid:02d}" == table_id:
                    table_name = name
                    break
            if table_name and len(table_examples[table_name]) < 3:  # Show max 3 examples per table
                table_examples[table_name].append(concept_id)
        
        for table_name, concept_ids in sorted(table_examples.items()):
            table_id = self.table_ids.get(table_name, 99)
            print(f"  {table_name} (ID {table_id:02d}): {', '.join(concept_ids)}")
            
        print(f"\nNote: Concept IDs use format XXYY where:")
        print(f"  XX = Table ID (2 digits)")
        print(f"  YY = Field order within table (2 digits)")
        print(f"  ZZ = Concept name ordering (2 digits)")
        
    def run(self) -> None:
        """Run the complete concept generation process."""
        print("Starting OMOP Concept Table Generation...")
        
        # Load configuration
        self.load_table_ids()
        concept_schema = self.load_concept_schema()
        
        # Extract concept fields from mapping
        self.extract_concept_id_fields()
        
        # Generate concept records
        self.create_concept_records(concept_schema)
        
        # Save results
        self.save_concept_table()
        
        # Print summary
        self.print_summary()
        
        print("\nConcept table generation completed successfully!")

def main():
    """Main entry point."""
    try:
        generator = OMOPConceptGenerator()
        generator.run()
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
