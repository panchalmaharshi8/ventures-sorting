# Data Conversion Utility UI

This is a frontend prototype for the Data Conversion Utility.

## How to Use

1. Open `index.html` in your web browser.
2. **Input Data**: Click the dashed box to select a folder containing your data files.
3. **Conversion Settings**:
   - Select the source format (CSV, JSON, etc.).
   - Select the target format (FHIR, OMOP, etc.).
   - If **OMOP** is selected, a "Concept Breakdown" button will appear.
4. **Manual Override**: Click to expand a JSON editor for custom mappings.
5. **Non-transferable Fields**: Click the link to view fields that will be dropped.
6. **Convert**: Check the "Confirm Anonymization" box to enable the Convert button.

## Tech Stack

- **HTML5**
- **Tailwind CSS** (via CDN)
- **Google Fonts** (Poppins & Open Sans)
