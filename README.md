# Movie_recommender
movie recommender system
import yaml
import os
import pandas as pd
from lxml import etree
import pyodbc

def load_config(path='config.yaml'):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def clean_and_parse(xml_file):
    with open(xml_file, 'rb') as f:
        content = f.read()
    start = content.find(b'<?xml')
    cleaned = content[start:] if start != -1 else content
    return etree.fromstring(cleaned)

def extract_common_fields(tree, common_paths, ns):
    def get(path):
        el = tree.find(path, namespaces=ns)
        return el.text.strip() if el is not None and el.text else 'NULL'
    return {k: get(v) for k, v in common_paths.items()}

def extract_data(tree, xpath_expr, ns, common_fields):
    elements = tree.xpath(xpath_expr, namespaces=ns)
    if not elements:
        print("[WARNING] No matching elements found.")
        return None

    data = []
    for elem in elements:
        row = {}
        for child in elem.iter():
            tag = child.tag.split('}')[-1]
            if child.text and child.text.strip():
                row[tag] = child.text.strip()
        if row:
            data.append(row)

    df = pd.DataFrame(data)
    for k, v in common_fields.items():
        df[k] = v
    return df

def save_to_db(df, table_name, db_config):
    try:
        conn = pyodbc.connect(
            Driver='{SQL Server}',
            Server=db_config['server'],
            Database=db_config['database'],
            Trusted_Connection='yes'
        )
        cursor = conn.cursor()

        cursor.execute(f"""
            IF OBJECT_ID('{table_name}', 'U') IS NULL
            BEGIN
                CREATE TABLE [{table_name}] ({','.join(f'[{col}] NVARCHAR(MAX)' for col in df.columns)})
            END
        """)

        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=?", (table_name,))
        existing_columns = set(row[0] for row in cursor.fetchall())
        for col in df.columns:
            if col not in existing_columns:
                cursor.execute(f"ALTER TABLE [{table_name}] ADD [{col}] NVARCHAR(MAX)")

        for _, row in df.iterrows():
            placeholders = ','.join(['?'] * len(row))
            column_names = ','.join(f'[{col}]' for col in row.index)
            insert_sql = f"INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})"
            cursor.execute(insert_sql, tuple(str(row[col]) if not pd.isna(row[col]) else None for col in row.index))

        conn.commit()
        print(f"[INFO] Inserted {len(df)} rows into {table_name}")
    except Exception as e:
        print(f"[ERROR] DB insert failed: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    config = load_config()
    ns = config['namespaces']
    db_config = config['database']

    # Take file path instead of directory
    file_path = input("Enter the full path to the XML file: ").strip()
    if not os.path.isfile(file_path):
        print("[ERROR] Invalid file path provided.")
        return

    print(f"\n[INFO] Processing file: {file_path}")
    tree = clean_and_parse(file_path)
    file_name = os.path.splitext(os.path.basename(file_path))[0]  # e.g. 'sample'

    for table_cfg in config['tables']:
        suffix = table_cfg.get('suffix', 'data')  # e.g. 'ntry' or 'bal'
        xpath_expr = table_cfg['xpath']
        common_paths = table_cfg['common_fields']

        table_name = f"{file_name}_{suffix}"  # Construct table name using file and suffix
        common_values = extract_common_fields(tree, common_paths, ns)
        df = extract_data(tree, xpath_expr, ns, common_values)

        if df is not None:
            save_to_db(df, table_name, db_config)

if __name__ == "__main__":
    main()
