# IHID to OMOP Mapping

This project contains tools to map IHID (Integrated Health Information Database) fields to the OMOP (Observational Medical Outcomes Partnership) Common Data Model structure.

## Overview

The OMOP Common Data Model is a standardized way to organize healthcare data. Although IHID contains de-identified data, it maintains consistent patient identifiers across tables. We map the IHID patient identifiers (patient_id or MRN) to the OMOP person_id as the root identifier, and encounter numbers (encntr_num) to visit_occurrence_id.

![OMOP Common Data Model Structure](OMOP_Structure.png)

The mapping process involves:
1. Analyzing the IHID data structure
2. Mapping IHID fields to corresponding OMOP tables and fields
3. Generating a mapping file for use in ETL processes
4. Validating the mapping for coverage and quality
5. Transforming IHID data to OMOP format

## Tools Included

### 1. IHID-OMOP Mapper (`ihid_omop_mapper.py`)

This script generates a mapping between IHID fields and OMOP fields based on the mapping information provided in the Excel file.

```bash
# Activate virtual environment with pandas and openpyxl
source ~/venv/bin/activate

# Run the mapper
python ihid_omop_mapper.py
```

The script will generate an `ihid_omop_mapping.json` file that can be used by the ETL process.

### 2. Mapping Validator (`mapping_validator.py`)

This tool validates the mapping by checking coverage of both IHID and OMOP fields.

```bash
# Activate virtual environment with pandas and openpyxl
source ~/venv/bin/activate

# Run the validator
python mapping_validator.py ihid_omop_mapping.json All_Tables_Combined.json OMOP_Summarized_Schema.xlsx
```

### 3. ETL Script (`ihid_etl.py`)

This script uses the mapping to extract, transform, and load data from IHID to OMOP format.

## File Details

- `All_Tables_Combined.json`: Contains metadata about the IHID tables and columns
- `OMOP_Summarized_Schema.xlsx`: Contains the OMOP schema with mappings to IHID fields
- `ihid_omop_mapping.json`: Generated mapping file (output of the mapper)
- `ihid_etl.py`: ETL script that uses the mapping to transform data

## OMOP Structure

The OMOP Common Data Model includes tables like:

1. **Person** - Maps to IHID's patient_id or MRN field as the root identifier
2. **Visit_Occurrence** - Maps to IHID admission/discharge records with encntr_num as the visit_occurrence_id
3. **Condition_Occurrence** - Maps to IHID diagnosis records, linked to person_id and visit_occurrence_id
4. **Drug_Exposure** - Maps to IHID medication records, linked to person_id and visit_occurrence_id
5. **Procedure_Occurrence** - Maps to IHID procedure records, linked to person_id and visit_occurrence_id
6. **Measurement** - Maps to IHID lab and other measurement records, linked to person_id and visit_occurrence_id
7. **Observation** - Maps to IHID observation records, linked to person_id and visit_occurrence_id
8. **Death** - Maps to IHID death records, linked to person_id

And other related tables for structured clinical data.

## Usage Instructions

1. Make sure you have the required dependencies installed:
   ```bash
   pip install pandas openpyxl
   ```

2. Run the mapper to generate the mapping file:
   ```bash
   python ihid_omop_mapper.py
   ```

3. Validate the mapping:
   ```bash
   python mapping_validator.py ihid_omop_mapping.json All_Tables_Combined.json OMOP_Summarized_Schema.xlsx
   ```

4. Use the mapping in the ETL process:
   ```bash
   python ihid_etl.py
   ```

## Notes

- The mapping is based on the Excel file `OMOP_Summarized_Schema.xlsx` which contains the correspondence between IHID and OMOP fields.
- Although IHID is de-identified, it maintains consistent patient identifiers (`patient_id` or `MRN`) across tables.
- We use these consistent patient identifiers as the OMOP `person_id` and the encounter numbers (`encntr_num`) as `visit_occurrence_id`.
- Not all IHID fields may have corresponding OMOP fields and vice versa.
