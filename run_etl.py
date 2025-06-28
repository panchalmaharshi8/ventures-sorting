#!/usr/bin/env python3
"""
Main ETL Pipeline Runner
Runs the complete IHID to OMOP transformation pipeline.
"""

import sys
import os
sys.path.append('scripts')

from optimized_ihid_etl import OptimizedIHIDToOMOPETL
import logging

def main():
    """Run the ETL pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    print("IHID to OMOP ETL Pipeline")
    print("=" * 40)
    print("This script will:")
    print("1. Load CSV data from the data/ directory")
    print("2. Apply IHID to OMOP field mappings")
    print("3. Transform data to OMOP format")
    print("4. Save results to omop_output/ directory")
    print()
    
    # Run the ETL
    etl = OptimizedIHIDToOMOPETL(
        data_dir='data',
        mapping_file='schemas/ihid_omop_mapping.json'
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
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
