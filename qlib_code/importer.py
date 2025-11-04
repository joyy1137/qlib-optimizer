import pandas as pd
import pymysql
from sqlalchemy import create_engine, text, types
import yaml
from typing import Dict, List, Optional
import os


class MySQLImporter:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.engine = create_engine(
            f"mysql+pymysql://{self.config['user']}:{self.config['password']}"
            f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
        )
    
    def create_table(self, table_name: str, schema: List[Dict]) -> None:

        columns = []
        for col_def in schema:
            field = col_def['field']
            dtype = col_def['type']
            columns.append(f"`{field}` {dtype}")
        
        # 生成CREATE TABLE语句
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {', '.join(columns)},
            PRIMARY KEY (`valuation_date`, `code`, `score_name`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(create_sql))
            conn.commit()

    def _table_exists(self, table_name: str) -> bool:
        sql = text("SELECT COUNT(*) AS cnt FROM information_schema.tables WHERE table_schema = :db AND table_name = :tbl")
        with self.engine.connect() as conn:
            # use scalar() to get the first column of the first row (count)
            scalar = conn.execute(sql, {
                'db': self.config['database'],
                'tbl': table_name
            }).scalar()
            try:
                return bool(scalar and int(scalar) > 0)
            except Exception:
                return False

    def _get_table_pk_columns(self, table_name: str) -> List[str]:
        sql = text("SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
                   "WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :tbl AND CONSTRAINT_NAME = 'PRIMARY' "
                   "ORDER BY ORDINAL_POSITION")
        with self.engine.connect() as conn:
            res = conn.execute(sql, {'db': self.config['database'], 'tbl': table_name}).fetchall()
            cols = []
            for row in res:
                # SQLAlchemy row may be a tuple or have a _mapping
                if hasattr(row, '_mapping'):
                    cols.append(row._mapping.get('COLUMN_NAME'))
                else:
                    # assume first element is the column name
                    cols.append(row[0])
            return [c for c in cols if c]

    def _count_duplicate_pk_groups(self, table_name: str, key_fields: List[str]) -> int:
        # Count number of duplicate key groups (groups having count > 1)
        keys = ', '.join([f'`{k}`' for k in key_fields])
        dup_sql = f"SELECT COUNT(*) AS dup_groups FROM (SELECT {keys}, COUNT(*) AS cnt FROM `{table_name}` GROUP BY {keys} HAVING cnt>1) t"
        with self.engine.connect() as conn:
            try:
                scalar = conn.execute(text(dup_sql)).scalar()
                return int(scalar) if scalar is not None else 0
            except Exception:
                # If the query fails (e.g., permission issues), return a conservative non-zero value to prevent inserts
                return 1
        
    def df_to_mysql(self, df: pd.DataFrame, table_name: str, schema: List[Dict]) -> None:

        df_clean = self._preprocess_data(df, schema)

        # Key fields that define uniqueness in the table
        key_fields = ['valuation_date', 'code', 'score_name']

        # Safety checks: If table exists but doesn't have the required primary key,
        # do NOT perform inserts. This prevents accidental duplicates in an existing
        # table that hasn't been cleaned yet.
        if self._table_exists(table_name):
            pk_cols = self._get_table_pk_columns(table_name)
            if not pk_cols or set(pk_cols) != set(key_fields):
                # If there are duplicate groups in the DB, abort and instruct user to clean first
                dup_count = self._count_duplicate_pk_groups(table_name, key_fields)
                
        else:
            # Table does not exist: create it (the create_table method defines the primary key)
            self.create_table(table_name, schema)

        # Deduplicate incoming DataFrame by key_fields, keep the latest by update_time when available
        if all(k in df_clean.columns for k in key_fields):
            if 'update_time' in df_clean.columns:
                try:
                    df_clean['update_time'] = pd.to_datetime(df_clean['update_time'])
                    df_clean = df_clean.sort_values('update_time').drop_duplicates(subset=key_fields, keep='last')
                except Exception:
                    df_clean = df_clean.drop_duplicates(subset=key_fields, keep='last')
            else:
                df_clean = df_clean.drop_duplicates(subset=key_fields, keep='last')

 
        # Always perform upsert: overwrite rows with same (valuation_date, code, score_name)
        records = df_clean.to_dict(orient='records')
        if not records:
            print("没有要插入的数据")
            return

        cols = list(df_clean.columns)
        col_list_sql = ', '.join([f"`{c}`" for c in cols])
        val_placeholders = ', '.join([f":{c}" for c in cols])

        non_key_cols = [c for c in cols if c not in key_fields]
        if non_key_cols:
            update_clause = ', '.join([f"`{c}`=VALUES(`{c}`)" for c in non_key_cols])
        else:
            update_clause = ', '.join([f"`{k}`=`{k}`" for k in key_fields])

        insert_sql = f"INSERT INTO `{table_name}` ({col_list_sql}) VALUES ({val_placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"

        try:
            with self.engine.begin() as conn:
                conn.execute(text(insert_sql), records)
            print(f"导入 {len(records)} 行到 {table_name}")
        except Exception as e:
            print(f"导入 失败: {e}")
    
    def _preprocess_data(self, df: pd.DataFrame, schema: List[Dict]) -> pd.DataFrame:
        """
        数据预处理：确保数据类型匹配
        """
        df_clean = df.copy()
        
        # 获取字段类型映射
        type_mapping = {col['field']: col['type'] for col in schema}
        
        for column in df_clean.columns:
            if column in type_mapping:
                mysql_type = type_mapping[column].upper()
                
         
                if 'DATETIME' in mysql_type or 'TIMESTAMP' in mysql_type:
                 
                    df_clean[column] = pd.to_datetime(df_clean[column])
                    # convert to python datetime objects which to_sql will insert as proper DATETIME
                    try:
                        df_clean[column] = [
                            (x.to_pydatetime() if hasattr(x, 'to_pydatetime') and not pd.isna(x) else None)
                            for x in df_clean[column]
                        ]
                    except Exception:
              
                        pass
                elif mysql_type == 'DATE' or (mysql_type.startswith('DATE') and 'DATETIME' not in mysql_type):
                    # convert to date only (no time part)
                    df_clean[column] = pd.to_datetime(df_clean[column]).dt.date
                elif 'DECIMAL' in mysql_type or 'FLOAT' in mysql_type or 'DOUBLE' in mysql_type:
                    df_clean[column] = pd.to_numeric(df_clean[column], errors='coerce')
                elif 'INT' in mysql_type:
                    df_clean[column] = pd.to_numeric(df_clean[column], errors='coerce').astype('Int64')
        
        return df_clean
    
    
    def close(self):
        """关闭连接"""
        self.engine.dispose()



if __name__ == "__main__":

    # 表结构定义
    schema = [
        {'field': 'valuation_date', 'type': 'DATE'},
        {'field': 'code', 'type': 'VARCHAR(50)'},
        {'field': 'final_score', 'type': 'DECIMAL(15,6)'},
        {'field': 'score_name', 'type': 'VARCHAR(50)'},
        {'field': 'update_time', 'type': 'DATETIME'}
    ]
    
    # 示例数据
    df = pd.DataFrame({
        'valuation_date': ['2025-01-01', '2025-01-01', '2025-01-02'],
        'code': ['000001', '000002', '000003'],
        'final_score': [95.5, 88.3, 92.7],
        'score_name': ['score_v1', 'score_v1', 'score_v1'],
        'update_time': ['2025-01-01 10:00:00', '2025-01-02 10:00:00', '2025-01-02 11:00:00']
    })
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'db.yaml'))
  
    

    importer = MySQLImporter(db_path)
    importer.df_to_mysql(df, 'qlib_scores', schema)
  
    
    importer.close()