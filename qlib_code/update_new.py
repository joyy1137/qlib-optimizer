"""
Load a trained model and run prediction for a target date using Qlib dataset handlers.

This script expects the following environment variables (set by `run_daily_update.py` or manually):
    TARGET_PREDICT_DATE  -- (optional) YYYY-MM-DD date string for prediction. Defaults to today.
"""

from importer import MySQLImporter
import os
from qlib.utils import init_instance_by_config
from qlib.workflow import R
from qlib.data import D
import pandas as pd
import qlib
from multiprocessing import freeze_support
import pickle
import sys
import os
import yaml
from datetime import datetime, time, date
original_sys_path = sys.path.copy()



def main():
   
    cfg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'paths.yaml'))

    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f) or {}

    provider_uri = cfg['provider_uri']
    qlib.init(provider_uri=provider_uri)

    model_path = cfg['model_path']

    model = pickle.load(open(model_path, "rb"))
    print("模型加载成功")
    

    instruments = D.instruments(market="all")
    handler_config = {
        "start_time": TARGET_PREDICT_DATE, 
        "end_time": TARGET_PREDICT_DATE,
        "instruments": instruments
    }

    dataset_config = {
        "class": "DatasetH",
        "module_path": "qlib.data.dataset",
        "kwargs": {
            "handler": {
                "class": "Alpha158",
                "module_path": "qlib.contrib.data.handler",
                "kwargs": handler_config
            },
            "segments": {
                "test": [TARGET_PREDICT_DATE, TARGET_PREDICT_DATE]
            }
        }
    }
    
    # 禁用并行处理
    os.environ["QLIB_DISABLE_MP"] = "1"  
    dataset = init_instance_by_config(dataset_config)
    test_df = dataset.prepare("test")
    pred = model.predict(dataset)
    pred_values = pred.values if isinstance(pred, pd.Series) else pred.ravel()

    pred_df = (
        pd.DataFrame({
            "valuation_date": test_df.index.get_level_values("datetime"),
            "code": test_df.index.get_level_values("instrument"),
            "final_score": pred_values,
            "score_name": "vp08",
            "update_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        .sort_values("final_score", ascending=False)
    )


    filename = f"prediction_{TARGET_PREDICT_DATE.replace('-', '')}.csv"
    OUTPUT_DIR = cfg['prediction_output_dir']
    output_path = os.path.join(OUTPUT_DIR, filename)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pred_df.to_csv(output_path, index=False)
    print(f"预测结果已保存到 {output_path}")

    db_yaml_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'db.yaml'))

    with open(db_yaml_path, 'r', encoding='utf-8') as f:
        db_cfg = yaml.safe_load(f) or {}

    importer = MySQLImporter(db_yaml_path)
    schema = [
        {'field': 'valuation_date', 'type': 'DATE'},
        {'field': 'code', 'type': 'VARCHAR(50)'},
        {'field': 'final_score', 'type': 'DECIMAL(15,6)'},
        {'field': 'score_name', 'type': 'VARCHAR(50)'},
        {'field': 'update_time', 'type': 'DATETIME'}
    ]

 
    importer.df_to_mysql(pred_df, db_cfg['table_name'], schema)
 
    print(f"预测结果已保存到数据库")


if __name__ == '__main__':
    # TARGET_PREDICT_DATE = ("2025-07-31")
    custom_path  = os.getenv('GLOBAL_TOOLSFUNC_test')
    sys.path.append(custom_path )
    import global_tools as gt
    date = datetime.now().time()
    date_str = datetime.now().strftime('%Y-%m-%d')
  
    if gt.is_workday(date_str) == False:
        TARGET_PREDICT_DATE = gt.last_workday_calculate(date_str)
    elif date <= time(19, 0):
        TARGET_PREDICT_DATE = gt.last_workday_calculate(date_str)
    else:
        TARGET_PREDICT_DATE = date_str
    sys.path = original_sys_path  
   
    freeze_support()  
    main()