import json
from collections import defaultdict

def load_ihid_catalog(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        rows = json.load(f)

    # Group columns by table
    catalog = defaultdict(list)
    for row in rows:
        table = row.get('Source_Section')
        col   = row.get('Column Name')
        if table and col:
            catalog[table].append({
                'name': col,
                'type': row.get('Data Type'),
                'explanation': row.get('Explanation')
            })
    return catalog

def main():
    catalog = load_ihid_catalog('All_Tables_Combined.json')

    for table, cols in catalog.items():
        print(f"{table}: {len(cols)} columns")
        
if __name__ == '__main__':
    main()
