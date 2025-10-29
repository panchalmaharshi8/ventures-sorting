#!/usr/bin/env python3
"""
Main ETL Pipeline Runner
Run IHID → OMOP (default) or IHID → FHIR based on --target.
"""

import sys
import os
sys.path.append('scripts')

from scripts.optimized_ihid_etl import OptimizedIHIDToOMOPETL
from scripts.optimized_ihid_fhir_etl import OptimizedIHIDToFHIRETL
import logging
import argparse

def main():
    """Run the ETL pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(description='Run IHID ETL to target data model')
    parser.add_argument('--target', choices=['omop', 'fhir'], default='omop', help='Target data model')
    parser.add_argument('--data-dir', default='data', help='Path to IHID CSV data directory')
    parser.add_argument('--mapping', default=None, help='Optional override for mapping file path')
    args = parser.parse_args()

    if args.target == 'omop':
        print("IHID to OMOP ETL Pipeline")
        print("=" * 40)
        print("This script will:")
        print("1. Load CSV data from the data/ directory")
        print("2. Apply IHID to OMOP field mappings")
        print("3. Transform data to OMOP format")
        print("4. Save results to omop_output/ directory")
        print()
        etl = OptimizedIHIDToOMOPETL(
            data_dir=args.data_dir,
            mapping_file=args.mapping or 'schemas/ihid_omop_mapping.json'
        )
        try:
            etl.run_etl()
            print()
            print("✅ ETL Pipeline completed successfully!")
            print("📁 Results saved to omop_output/ directory")
            print("📊 Check the logs above for processing statistics")
        except Exception as e:
            print(f"❌ ETL Pipeline failed: {e}")
            return 1
    else:
        print("IHID to FHIR ETL Pipeline")
        print("=" * 40)
        print("This script will:")
        print("1. Load CSV data from the data/ directory")
        print("2. Apply IHID to FHIR field mappings")
        print("3. Transform data to simplified FHIR JSON resources")
        print("4. Save results to fhir_output/ directory")
        print()
        etl = OptimizedIHIDToFHIRETL(
            data_dir=args.data_dir,
            mapping_file=args.mapping or 'schemas/ihid_fhir_mapping.json'
        )
        try:
            etl.run_etl()
            print()
            print("✅ FHIR ETL Pipeline completed successfully!")
            print("📁 Results saved to fhir_output/ directory")
            print("📊 Check the logs above for processing statistics")
        except Exception as e:
            print(f"❌ FHIR ETL Pipeline failed: {e}")
            return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
