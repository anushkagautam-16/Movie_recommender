# Movie_recommender
movie recommender system

import yaml
import os
import pandas as pd
from lxml import etree
import pyodbc
from datetime import datetime

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

def create_stats_table(cursor):
    cursor.execute("""
        IF OBJECT_ID('Stats', 'U') IS NULL
        BEGIN
            CREATE TABLE Stats (
                ID INT IDENTITY(1,1) PRIMARY KEY,
                FileName NVARCHAR(255),
                Staging_Table_Name NVARCHAR(255),
                System_Date DATETIME,
                StartDate DATETIME,
                End_Date DATETIME,
                Count INT,
                Status NVARCHAR(50)
            )
        END
    """)

def insert_stats_row(cursor, filename, table_name, start_time, end_time, count, status):
    cursor.execute("""
        INSERT INTO Stats (FileName, Staging_Table_Name, System_Date, StartDate, End_Date, Count, Status)
        VALUES (?, ?, GETDATE(), ?, ?, ?, ?)
    """, filename, table_name, start_time, end_time, count, status)

def save_to_db(df, table_name, db_config, file_name, dataload_id):
    start_time = datetime.now()
    try:
        conn = pyodbc.connect(
            Driver='{SQL Server}',
            Server=db_config['server'],
            Database=db_config['database'],
            Trusted_Connection='yes'
        )
        cursor = conn.cursor()

        # Add dataload_id to dataframe
        df['dataload_id'] = dataload_id

        # Create table if it doesn't exist
        cursor.execute(f"""
            IF OBJECT_ID('{table_name}', 'U') IS NULL
            BEGIN
                CREATE TABLE [{table_name}] (
                    {','.join(f'[{col}] NVARCHAR(MAX)' for col in df.columns if col != 'dataload_id')},
                    dataload_id INT,
                    FOREIGN KEY (dataload_id) REFERENCES Stats(ID)
                )
            END
        """)

        # Add missing columns
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=?", (table_name,))
        existing_columns = set(row[0] for row in cursor.fetchall())
        for col in df.columns:
            if col not in existing_columns:
                if col == "dataload_id":
                    cursor.execute(f"ALTER TABLE [{table_name}] ADD dataload_id INT")
                    cursor.execute(f"""
                        ALTER TABLE [{table_name}] ADD CONSTRAINT FK_{table_name}_Stats
                        FOREIGN KEY (dataload_id) REFERENCES Stats(ID)
                    """)
                else:
                    cursor.execute(f"ALTER TABLE [{table_name}] ADD [{col}] NVARCHAR(MAX)")

        # Insert data
        for _, row in df.iterrows():
            placeholders = ','.join(['?'] * len(row))
            column_names = ','.join(f'[{col}]' for col in row.index)
            insert_sql = f"INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})"
            cursor.execute(insert_sql, tuple(str(row[col]) if not pd.isna(row[col]) else None for col in row.index))

        row_count = len(df)
        conn.commit()

        # Update status to Complete in Stats table
        cursor.execute("""
            UPDATE Stats
            SET End_Date = ?, Count = ?, Status = ?
            WHERE ID = ?
        """, datetime.now(), row_count, "Complete" if row_count > 0 else "Empty", dataload_id)
        conn.commit()

        print(f"[INFO] Inserted {row_count} rows into {table_name}")

    except Exception as e:
        print(f"[ERROR] DB insert failed: {e}")
        if 'cursor' in locals():
            cursor.execute("""
                UPDATE Stats SET Status = ?, End_Date = GETDATE() WHERE ID = ?
            """, 'Failed', dataload_id)
            conn.commit()
    finally:
        if 'conn' in locals():
            conn.close()

def main():
    config = load_config()
    ns = config['namespaces']
    db_config = config['database']

    file_path = input("Enter the full path to the XML file: ").strip()
    if not os.path.isfile(file_path):
        print("[ERROR] Invalid file path provided.")
        return

    print(f"\n[INFO] Processing file: {file_path}")
    tree = clean_and_parse(file_path)
    file_name = os.path.splitext(os.path.basename(file_path))[0]

    conn = pyodbc.connect(
        Driver='{SQL Server}',
        Server=db_config['server'],
        Database=db_config['database'],
        Trusted_Connection='yes'
    )
    cursor = conn.cursor()
    create_stats_table(cursor)

    # Insert initial 'in_progress' status
    start_time = datetime.now()
    cursor.execute("""
        INSERT INTO Stats (FileName, Staging_Table_Name, System_Date, StartDate, Status)
        VALUES (?, NULL, GETDATE(), ?, ?)
    """, file_name, start_time, 'In Progress')
    conn.commit()

    cursor.execute("SELECT SCOPE_IDENTITY()")
    dataload_id = cursor.fetchone()[0]
    conn.close()

    for table_cfg in config['tables']:
        suffix = table_cfg.get('suffix', 'data')
        xpath_expr = table_cfg['xpath']
        common_paths = table_cfg['common_fields']

        table_name = f"{file_name}_{suffix}"
        common_values = extract_common_fields(tree, common_paths, ns)
        df = extract_data(tree, xpath_expr, ns, common_values)

        if df is not None:
            save_to_db(df, table_name, db_config, file_name, dataload_id)

if __name__ == "__main__":
    main()
