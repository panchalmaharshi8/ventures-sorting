"""
data_converter_server/etl_runner.py

Importable ETL entrypoints for use by the Flask server.

Guarantees:
- Input preflight: ensure there is at least one CSV to process
- Optional preprocessing: convert .xlsx/.xls -> .csv in the job input folder
- Run ETL with job-scoped CWD (so any relative outputs land inside the job directory)
- Postflight: require output artifacts exist (avoid false "success")
"""

import os
import sys
import logging
from typing import Optional, Tuple, List
from contextlib import contextmanager

# data_converter_server/ is one level below repo root
SERVER_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SERVER_DIR)

# Ensure repo root is importable (so "scripts.*" imports work)
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.optimized_ihid_etl import OptimizedIHIDToOMOPETL
from scripts.optimized_ihid_fhir_etl import OptimizedIHIDToFHIRETL


@contextmanager
def _pushd(path: str):
    prev = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev)


def _list_files_with_ext(dir_path: str, exts: List[str]) -> List[str]:
    out = []
    for name in os.listdir(dir_path):
        p = os.path.join(dir_path, name)
        if os.path.isfile(p):
            _, ext = os.path.splitext(name)
            if ext.lower() in exts:
                out.append(p)
    return sorted(out)


def _xlsx_to_csvs(input_dir: str) -> Tuple[int, str, List[str]]:
    """
    Convert any .xlsx/.xls in input_dir into CSV(s) in the same folder.
    One CSV per sheet, named: <base>__<sheet>.csv
    Returns: (exit_code, message, created_files)
    """
    xls_files = _list_files_with_ext(input_dir, [".xlsx", ".xls"])
    if not xls_files:
        return 0, "No Excel files to convert.", []

    try:
        import pandas as pd  # uses openpyxl under the hood for xlsx
    except Exception as e:
        return 1, f"Excel conversion requires pandas + openpyxl. Import failed: {e}", []

    created = []
    for xls_path in xls_files:
        base = os.path.splitext(os.path.basename(xls_path))[0]

        try:
            xl = pd.ExcelFile(xls_path)
            sheet_names = xl.sheet_names
        except Exception as e:
            return 1, f"Failed reading Excel file '{os.path.basename(xls_path)}': {e}", created

        for sheet in sheet_names:
            try:
                df = xl.parse(sheet_name=sheet)
            except Exception as e:
                return 1, f"Failed parsing sheet '{sheet}' in '{os.path.basename(xls_path)}': {e}", created

            # Sanitize sheet name for filename
            safe_sheet = "".join(ch for ch in sheet if ch.isalnum() or ch in (" ", "_", "-")).strip()
            safe_sheet = safe_sheet.replace(" ", "_") or "Sheet"

            out_name = f"{base}__{safe_sheet}.csv"
            out_path = os.path.join(input_dir, out_name)

            # Avoid overwriting
            if os.path.exists(out_path):
                i = 2
                while True:
                    cand = os.path.join(input_dir, f"{base}__{safe_sheet}_{i}.csv")
                    if not os.path.exists(cand):
                        out_path = cand
                        out_name = os.path.basename(cand)
                        break
                    i += 1

            try:
                df.to_csv(out_path, index=False)
            except Exception as e:
                return 1, f"Failed writing CSV '{out_name}': {e}", created

            created.append(out_name)

    return 0, f"Converted {len(xls_files)} Excel file(s) into {len(created)} CSV(s).", created


def _dir_has_any_files(dir_path: str) -> bool:
    if not os.path.isdir(dir_path):
        return False
    # Walk fully through directories to find at least one file
    for root, dirs, files in os.walk(dir_path):
        if files:
            return True
    return False


def run_etl_job(
    target: str,
    data_dir: str,
    mapping_file: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> Tuple[int, str]:
    """
    Run ETL for a specific job.

    Args:
      target: 'omop' or 'fhir'
      data_dir: runs/<job_id>/input
      mapping_file: optional mapping json path override
      output_dir: runs/<job_id>/output (preferred)

    Returns:
      (exit_code, message)
    """
    target = (target or "omop").strip().lower()
    if target not in ("omop", "fhir"):
        target = "omop"

    data_dir = os.path.abspath(data_dir)
    if not os.path.isdir(data_dir):
        return 1, f"data_dir not found: {data_dir}"

    if output_dir:
        output_dir = os.path.abspath(output_dir)
        os.makedirs(output_dir, exist_ok=True)

    logging.getLogger().setLevel(logging.INFO)

    # ---------
    # Preflight: ensure CSV exists; if not, try Excel->CSV conversion
    # ---------
    csvs = _list_files_with_ext(data_dir, [".csv"])
    if not csvs:
        xls = _list_files_with_ext(data_dir, [".xlsx", ".xls"])
        if xls:
            code, msg, created = _xlsx_to_csvs(data_dir)
            if code != 0:
                return 1, f"Preprocess failed: {msg}"
            # refresh CSV list
            csvs = _list_files_with_ext(data_dir, [".csv"])
            if not csvs:
                return 1, f"Preprocess ran but still no CSVs found. Details: {msg}"
        else:
            return 1, "No CSV files found in input directory. Upload IHID CSV exports (or Excel files to auto-convert)."

    # ---------
    # Build ETL
    # ---------
    if target == "omop":
        mapping = mapping_file or os.path.join(REPO_ROOT, "schemas", "ihid_omop_mapping.json")
        etl = OptimizedIHIDToOMOPETL(data_dir=data_dir, mapping_file=mapping)
        fixed_out_name = "omop_output"
    else:
        mapping = mapping_file or os.path.join(REPO_ROOT, "schemas", "ihid_fhir_mapping.json")
        etl = OptimizedIHIDToFHIRETL(data_dir=data_dir, mapping_file=mapping)
        fixed_out_name = "fhir_output"

    # Determine job_dir from data_dir (runs/<job_id>/input -> runs/<job_id>)
    job_dir = os.path.dirname(data_dir)

    # ---------
    # Run ETL job-scoped
    # ---------
    try:
        with _pushd(job_dir):
            etl.run_etl()
    except Exception as e:
        return 1, f"ETL failed (target={target}): {e}"

    # ---------
    # Postflight: require output artifacts exist
    # ---------
    produced_dir = os.path.join(job_dir, fixed_out_name)

    # Case 1: ETL wrote to fixed relative dir inside job_dir
    if os.path.isdir(produced_dir) and _dir_has_any_files(produced_dir):
        # Normalize to output_dir if provided
        if output_dir:
            dest = os.path.join(output_dir, fixed_out_name)
            os.makedirs(dest, exist_ok=True)

            for name in os.listdir(produced_dir):
                src_path = os.path.join(produced_dir, name)
                dst_path = os.path.join(dest, name)

                if os.path.isdir(src_path):
                    if not os.path.exists(dst_path):
                        os.rename(src_path, dst_path)
                    else:
                        for sub in os.listdir(src_path):
                            os.rename(os.path.join(src_path, sub), os.path.join(dst_path, sub))
                        os.rmdir(src_path)
                else:
                    if os.path.exists(dst_path):
                        base, ext = os.path.splitext(name)
                        i = 2
                        while True:
                            cand = os.path.join(dest, f"{base}_{i}{ext}")
                            if not os.path.exists(cand):
                                dst_path = cand
                                break
                            i += 1
                    os.rename(src_path, dst_path)

            try:
                os.rmdir(produced_dir)
            except OSError:
                pass

        return 0, f"ETL completed successfully (target={target})."

    # Case 2: ETL wrote directly somewhere else (or nowhere). If output_dir has anything, accept.
    if output_dir and _dir_has_any_files(output_dir):
        return 0, f"ETL completed successfully (target={target})."

    return 1, (
        f"ETL ran without exception but produced no output artifacts. "
        f"Check that inputs are valid IHID CSVs. "
        f"CSV count seen: {len(csvs)}"
    )
