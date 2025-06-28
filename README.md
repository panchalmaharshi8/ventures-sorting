# IHID to OMOP Mapping

Automated mapping and ETL pipeline for transforming IHID (Integrated Health Information Database) data to the OMOP (Observational Medical Outcomes Partnership) Common Data Model.

## Overview

This project provides tools to map IHID fields to OMOP CDM tables and fields, generate mapping files, and transform healthcare data while maintaining consistent patient and encounter identifiers across tables.

## Features

- **Automated Mapping**: Generates field mappings from IHID to OMOP based on schema definitions
- **Robust ETL**: Handles missing data, table name variations, and identifier fallbacks
- **Validation Tools**: Comprehensive mapping coverage analysis and quality checks
- **Error Recovery**: Graceful handling of missing fields and database inconsistencies

## Requirements

- Python 3.7+
- pandas
- openpyxl

## Usage

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Generate Mapping
```bash
python ihid_omop_mapper.py
```

### 3. Validate Mapping
```bash
python mapping_validator.py ihid_omop_mapping.json All_Tables_Combined.json OMOP_Summarized_Schema.xlsx
```

### 4. Run ETL
```bash
python ihid_etl.py
```

## Scripts

- **`ihid_omop_mapper.py`** - Generates mapping from IHID to OMOP fields based on schema files
- **`mapping_validator.py`** - Validates mapping coverage and identifies gaps
- **`ihid_etl.py`** - Transforms IHID data to OMOP format using the generated mapping
- **`create_sample_db.py`** - Creates sample database for testing (optional)
