import json
import sqlite3
from collections import defaultdict

# loading IHID json
def load_catalog(catalog_path):
    with open(catalog_path, 'r', encoding='utf-8') as f:
        rows = json.load(f)
    catalog = defaultdict(list)
    for r in rows:
        tbl = r['Source_Section']
        col = r['Column Name']
        catalog[tbl].append(col)
    return catalog

#loading mapping json (we will create this once the mapping is completed)
def load_mapping(mapping_path):
    try:
        with open(mapping_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}  # no mappings yet

# function to fetch any non-null records from the table for a given table and field
def fetch_non_null(conn, table, key_field, key_value, columns):
    col_list = ", ".join(columns)
    sql = f"SELECT {col_list} FROM {table} WHERE {key_field} = ?"
    cursor = conn.execute(sql, (key_value,))
    records = []
    for row in cursor.fetchall():
        # build a sparse dict of only populated fields
        rec = {col: val for col, val in zip(columns, row) if val is not None}
        if rec:
            records.append(rec)
    return records

# fetches everything for a given encounter number, starting from admission/discharge and then other child tables
def fetch_encounter(conn, encntr_num, catalog):
    details = {}
    # First, all tables keyed by encntr_num
    for table, cols in catalog.items():
        # skip DAD_Diagnosis / DAD_Intervention for now; they key on abstract_num
        if table in ('DAD_Diagnosis', 'DAD_Intervention'):
            continue
        if 'encntr_num' not in cols:
            continue
        recs = fetch_non_null(conn, table, 'encntr_num', encntr_num, cols)
        if recs:
            details[table] = recs

    # Now handle CIHI DAD children via abstract_num
    if 'DAD_Abstract' in catalog:
        abs_cols = catalog['DAD_Abstract']
        abstracts = fetch_non_null(conn, 'DAD_Abstract', 'encntr_num', encntr_num, abs_cols)
        if abstracts:
            details['DAD_Abstract'] = abstracts
            for absr in abstracts:
                abs_id = absr.get('abstract_num')
                for child in ('DAD_Diagnosis', 'DAD_Intervention'):
                    child_cols = catalog.get(child, [])
                    if not child_cols or 'abstract_num' not in child_cols:
                        continue
                    recs = fetch_non_null(conn, child, 'abstract_num', abs_id, child_cols)
                    if recs:
                        details.setdefault(child, []).extend(recs)

    return details

def main():
    # paths
    catalog_path = 'All_Tables_Combined.json'
    mapping_path = 'mapping.json'
    db_path      = 'IHID.db'  # TOY DATASET NEEDED HERE

    # load
    catalog = load_catalog(catalog_path)
    mapping = load_mapping(mapping_path)

    # connect
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # iterate encounters
    for row in conn.execute("SELECT encntr_num FROM admission_discharge"):
        enc = row['encntr_num']
        details = fetch_encounter(conn, enc, catalog)

        # inspect unmapped fields
        for table, recs in details.items():
            for rec in recs:
                for col, val in rec.items():
                    mapped = mapping.get(table, {}).get(col)
                    if not mapped:
                        print(f"[UNMAPPED] {table}.{col} = {val!r}")

        # at this point, `details` is a dict of:
        #   table → [ { col1: val1, col2: val2, … }, … ]
        # you can now pass `details` into your mapping/ETL layer

    conn.close()

if __name__ == '__main__':
    main()
