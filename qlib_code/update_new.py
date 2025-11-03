"""
Load a trained model and run prediction for a target date using Qlib dataset handlers.

This script expects the following environment variables (set by `run_daily_update.py` or manually):
    TARGET_PREDICT_DATE  -- (optional) YYYY-MM-DD date string for prediction. Defaults to today.
"""

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
from datetime import datetime, timedelta, date
original_sys_path = sys.path.copy()



def main():
   
    model_path = r"E:\qlib_code\mlruns\505608931795866282\7d35e7a5b04149f690fd8e0cf031f84d\artifacts\trained_model"
    model = pickle.load(open(model_path, "rb"))
    print("模型加载成功")
    custom_path  = os.getenv('GLOBAL_TOOLSFUNC_test')
    sys.path.append(custom_path )
    import global_tools as gt
    pre_date = gt.last_workday_calculate(TARGET_PREDICT_DATE)
    sys.path = original_sys_path


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
            "code": test_df.index.get_level_values("instrument"),
            "score": pred_values,
        })
        .sort_values("score", ascending=False)
    )

    filename = f"prediction_{TARGET_PREDICT_DATE.replace('-', '')}.csv"
    OUTPUT_DIR = r"E:\\qlib_output"
    output_path = os.path.join(OUTPUT_DIR, filename)
    pred_df.to_csv(output_path, index=False)
    print(f"预测结果已保存到 {output_path}")

if __name__ == '__main__':
    # TARGET_PREDICT_DATE = date.today().strftime('%Y-%m-%d')
    TARGET_PREDICT_DATE = '2025-10-31' 

    provider_uri = r"E:\qlib_data\tushare_qlib_data\qlib_bin"
    qlib.init(provider_uri=provider_uri)
    
    freeze_support()  
    main()