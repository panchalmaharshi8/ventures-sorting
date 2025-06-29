# IHID to OMOP ETL Pipeline

Automated transformation of IHID (Integrated Health Information Datalab) data to OMOP (Observational Medical Outcomes Partnership) Common Data Model format.

## Quick Start

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Add Your Data**
   - Place CSV files in the `data/` directory
   - Ensure files follow the expected naming convention (e.g., `1. dad_information.csv`)

3. **Run ETL Pipeline**
   ```bash
   python run_etl.py
   ```

The pipeline will process your CSV data and generate OMOP-formatted JSON files in the `omop_output/` directory.

## Project Structure

```
├── run_etl.py                    # Main pipeline runner
├── requirements.txt              # Python dependencies
├── data/                         # Input CSV files (user provided)
├── scripts/                      # Processing scripts
│   ├── optimized_ihid_etl.py          # Core ETL engine
│   ├── enhanced_ihid_omop_mapper.py   # Field mapping generator
│   ├── mapping_validator.py           # Validation utilities
│   └── update_catalog_from_csvs.py    # Schema updater
├── schemas/                      # Configuration files
│   ├── ihid_omop_mapping.json         # Field mappings
│   ├── All_Tables_Combined.json       # Data catalog
│   └── OMOP_Structure.png             # OMOP diagram
└── archive/                      # Previous versions
```

## Generated Output

The pipeline creates standard OMOP CDM tables:
- `person.json` - Patient demographics
- `visit_occurrence.json` - Healthcare encounters  
- `condition_occurrence.json` - Diagnoses
- `procedure_occurrence.json` - Medical procedures
- `drug_exposure.json` - Medications
- `care_site.json` - Healthcare facilities
- `death.json` - Mortality data
- `visit_detail.json` - Detailed visit information
- `cost.json` - Healthcare costs

## Advanced Usage

**Update Data Catalog:**
```bash
python scripts/update_catalog_from_csvs.py
```

**Regenerate Mappings:**
```bash
python scripts/enhanced_ihid_omop_mapper.py
```

**Validate Mappings:**
```bash
python scripts/mapping_validator.py schemas/ihid_omop_mapping.json schemas/All_Tables_Combined.json
```

## Requirements

- Python 3.7+
- pandas
- openpyxl
