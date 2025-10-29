# IHID to OMOP or FHIR ETL Pipeline

Automated transformation of IHID (Integrated Health Information Datalab) data to:
- OMOP (Observational Medical Outcomes Partnership) Common Data Model format, or
- FHIR (Fast Healthcare Interoperability Resources) JSON resources.

## Quick Start

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Add Your Data**
   - Place CSV files in the `data/` directory
   - Ensure files follow the expected naming convention (e.g., `1. dad_information.csv`)

3. **Run ETL Pipeline (OMOP default)**
   ```bash
   python run_etl.py
   ```

   Or generate FHIR resources instead of OMOP tables:

   ```bash
   python run_etl.py --target fhir
   ```

The pipeline will process your CSV data and generate:
- OMOP-formatted JSON files in `omop_output/` (default), or
- FHIR resource JSON files in `fhir_output/` when `--target fhir` is used.

## Project Structure

```
├── run_etl.py                    # Main pipeline runner
├── requirements.txt              # Python dependencies
├── data/                         # Input CSV files (user provided)
├── scripts/                      # Processing scripts
│   ├── optimized_ihid_etl.py          # Core OMOP ETL engine
│   ├── optimized_ihid_fhir_etl.py     # Core FHIR ETL engine
│   ├── enhanced_ihid_omop_mapper.py   # OMOP field mapping generator
│   ├── enhanced_ihid_fhir_mapper.py   # FHIR field mapping generator
│   ├── mapping_validator.py           # Validation utilities
│   └── update_catalog_from_csvs.py    # Schema updater
├── schemas/                      # Configuration files
│   ├── ihid_omop_mapping.json         # OMOP field mappings
│   ├── ihid_fhir_mapping.json         # FHIR field mappings (generated)
│   ├── All_Tables_Combined.json       # Data catalog
│   ├── OMOP_Summarized_Schema.xlsx    # OMOP summarized schema
│   └── FIHR_Summarized_Schema.xlsx    # FHIR summarized schema (5 sheets)
└── archive/                      # Previous versions
```

## Generated Output

When targeting OMOP, the pipeline creates standard CDM tables:
- `person.json` - Patient demographics
- `visit_occurrence.json` - Healthcare encounters  
- `condition_occurrence.json` - Diagnoses
- `procedure_occurrence.json` - Medical procedures
- `drug_exposure.json` - Medications
- `care_site.json` - Healthcare facilities
- `death.json` - Mortality data
- `visit_detail.json` - Detailed visit information
- `cost.json` - Healthcare costs

When targeting FHIR, the pipeline creates resource arrays by resource type:
- `patient.json`, `encounter.json`, `procedure.json`, `observation.json`, etc.

## Advanced Usage

**Update Data Catalog:**
```bash
python scripts/update_catalog_from_csvs.py
```

**Regenerate Mappings:**
```bash
python scripts/enhanced_ihid_omop_mapper.py
```

Generate the IHID→FHIR mapping from the summarized schema:
```bash
python scripts/enhanced_ihid_fhir_mapper.py
```

**Validate Mappings:**
```bash
python scripts/mapping_validator.py schemas/ihid_omop_mapping.json schemas/All_Tables_Combined.json
```

## Requirements

- Python 3.7+
- pandas
- openpyxl
