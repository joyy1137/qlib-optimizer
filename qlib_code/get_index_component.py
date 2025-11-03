import pymysql
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os


def process_index_data(host, username, password, dbname, port=3306, organization='zz500', start_date='2020-01-01'):
   
    if isinstance(host, str) and ':' in host:
        host_only, port_str = host.split(':', 1)
        host = host_only
        try:
            port = int(port_str)
        except Exception:
            port = port

    password_quoted = quote_plus(password)
    engine_url = f"mysql+pymysql://{username}:{password_quoted}@{host}:{port}/{dbname}"
    engine = create_engine(engine_url)

    query = """
    SELECT valuation_date, code 
    FROM data_indexcomponent 
    WHERE organization = %s 
    AND valuation_date >= %s
    """
    db = pd.read_sql(query, engine, params=(organization, start_date))

    engine.dispose()
  
    db['valuation_date'] = pd.to_datetime(db['valuation_date'])
    
    result = (
        db.groupby('code')['valuation_date']
        .agg(['min', 'max']) 
        .reset_index()
        .rename(columns={'min': 'start_date', 'max': 'end_date'})
    )
    
    result = result.sort_values('code')
    
    return result

def save_to_file(result, output_path):
    
    result.to_csv(output_path, sep='\t', header=False, index=False)

if __name__ == '__main__':
 
    db_config = {
        'host': 'rm-bp1o6we7s3o1h76x1to.mysql.rds.aliyuncs.com',
        'port': 3306,
        'dbname': 'data_prepared_new',
        'username': 'kai',
        'password': 'Abcd1234#'
    }
    
    output_path = r"E:\qlib_data\tushare_qlib_data\qlib_bin\instruments\csi2000.txt"
    parent_dir = os.path.dirname(output_path)
    os.makedirs(parent_dir, exist_ok=True)

    result = process_index_data(host = db_config['host'], username=db_config['username'], 
                                password=db_config['password'], dbname=db_config['dbname'], 
                                port=db_config['port'], organization='zz2000', start_date='2015-01-01')

    save_to_file(result, output_path)
    
    print(f"数据处理完成，已保存到: {output_path}")