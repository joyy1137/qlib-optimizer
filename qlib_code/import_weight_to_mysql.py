from pathlib import Path
import pandas as pd
import os
from importer import MySQLImporter
import yaml

DEFAULT_DB_CONFIG = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'db.yaml'))
path_config = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'paths.yaml'))
with open(path_config, 'r', encoding='utf-8') as f:
        path = yaml.safe_load(f) or {}
A_FOLDER = path['temp_dir']


SCHEMA = [
    {'field': 'valuation_date', 'type': 'VARCHAR(50)'},
    {'field': 'code', 'type': 'VARCHAR(50)'},
    {'field': 'portfolio_name', 'type': 'VARCHAR(50)'},
    {'field': 'weight', 'type': 'FLOAT'},
    {'field': 'update_time', 'type': 'DATETIME'}
]


def discover_csv_files(a_folder: str):
    p = Path(a_folder)
    if not p.exists():
        raise FileNotFoundError(f"a folder not found: {a_folder}")
    return sorted([str(x) for x in p.glob('*.csv') if x.is_file()])


def read_and_normalize(csv_path: str):
    df = pd.read_csv(csv_path, dtype=str)
    # normalize column names
    df.columns = [c.strip() for c in df.columns]

    # ensure required columns exist
    required = ['valuation_date', 'code', 'portfolio_name', 'weight']
    lower_map = {c.lower(): c for c in df.columns}

    # Try to map case-insensitive
    mapped = {}
    for req in required:
        if req in df.columns:
            mapped[req] = req
        elif req in lower_map:
            mapped[req] = lower_map[req]
        else:
            raise KeyError(f"CSV {csv_path} 缺少必要列: {req}")

    # select and rename
    df2 = df[[mapped[r] for r in required]].copy()
    df2.columns = required

    # convert types
    df2['valuation_date'] = df2['valuation_date'].astype(str)
    df2['code'] = df2['code'].astype(str)
    df2['portfolio_name'] = df2['portfolio_name'].astype(str)
    # weight to numeric
    df2['weight'] = pd.to_numeric(df2['weight'], errors='coerce')

    # add upload timestamp column for all rows
    try:
        df2['update_time'] = pd.Timestamp.now()
    except Exception:
        # fallback to python datetime
        from datetime import datetime
        df2['update_time'] = datetime.now()

    return df2


def main():
    cfg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'db.yaml'))
    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f) or {}

    table = cfg['table_name2']
    pk = cfg.get('pk', 'valuation_date,code,portfolio_name')
    a_folder = cfg.get('a_folder', A_FOLDER)

    pk_fields = [p.strip() for p in pk.split(',') if p.strip()]

    csvs = discover_csv_files(a_folder)
    if not csvs:
        print(f"在 {a_folder} 中未找到 csv 文件。")
        return
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'db.yaml'))
  
    importer = MySQLImporter(db_path)

    all_rows = []
    for csv in csvs:
        df = read_and_normalize(csv)
        all_rows.append(df)

    if not all_rows:
        print("没有读取到任何数据。")
        importer.close()
        return

    combined = pd.concat(all_rows, ignore_index=True)

    # call df_to_mysql with schema and pk_fields
    importer.df_to_mysql(combined, table, SCHEMA, pk_fields, database='database2')
   
    importer.close()


if __name__ == '__main__':
    main()
