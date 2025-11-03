"""Export test-set predictions as one CSV per day.

Usage examples:
    # default: uses model path and full test range from workflow_config (hardcoded defaults)
    python export_test_scores_per_day.py --model-path "E:\\qlib_code\\mlruns\\505608931795866282\\7d35e7a5b04149f690fd8e0cf031f84d\\artifacts\\trained_model" \
        --start-date 2020-09-01 --end-date 2025-10-22 --output-dir "E:\\qlib_output"

This will write files like `prediction_20200901.csv`, `prediction_20200902.csv`, ...
Each CSV contains columns: code, score
"""
import os
import argparse
import pickle
from datetime import datetime
import pandas as pd
import qlib
from qlib.utils import init_instance_by_config
from qlib.data import D


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--model-path", default=r"E:\qlib_code\mlruns\505608931795866282\7d35e7a5b04149f690fd8e0cf031f84d\artifacts\trained_model")
    p.add_argument("--provider-uri", default=r"E:\\qlib_data\\tushare_qlib_data\\qlib_bin", help="Qlib provider uri")
    p.add_argument("--start-date", default="2020-09-01", help="Test start date YYYY-MM-DD")
    p.add_argument("--end-date", default="2025-10-22", help="Test end date YYYY-MM-DD")
    p.add_argument("--output-dir", default=r"E:\\qlib_output", help="Directory to save daily CSVs")
    p.add_argument("--instruments", default="all", help="Market instruments argument passed to D.instruments (default 'all')")
    return p.parse_args()


def main():
    args = parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    # initialize qlib provider first so D.instruments works
    qlib.init(provider_uri=args.provider_uri)

    print("Loading model:", args.model_path)
    with open(args.model_path, "rb") as f:
        model = pickle.load(f)

    instruments = D.instruments(market=args.instruments)

    handler_config = {
        "start_time": args.start_date,
        "end_time": args.end_date,
        "instruments": instruments,
    }

    dataset_config = {
        "class": "DatasetH",
        "module_path": "qlib.data.dataset",
        "kwargs": {
            "handler": {
                "class": "Alpha158",
                "module_path": "qlib.contrib.data.handler",
                "kwargs": handler_config,
            },
            "segments": {
                "test": [args.start_date, args.end_date],
            },
        },
    }

    # Disable qlib multiprocessing for predict stability
    os.environ["QLIB_DISABLE_MP"] = "1"

    dataset = init_instance_by_config(dataset_config)
    test_df = dataset.prepare("test")

    # Run prediction across the full test dataset
    print("Generating predictions for test set...")
    pred = model.predict(dataset)
    pred_values = pred.values if isinstance(pred, pd.Series) else pred.ravel()

    # Recover index levels for datetime and instrument
    idx_names = test_df.index.names
    try:
        datetimes = test_df.index.get_level_values("datetime")
    except Exception:
        datetimes = test_df.index.get_level_values(0)

    try:
        instruments_idx = test_df.index.get_level_values("instrument")
    except Exception:
        # assume instrument is second level
        instruments_idx = test_df.index.get_level_values(1)

    df = pd.DataFrame({
        "datetime": pd.to_datetime(datetimes),
        "code": instruments_idx,
        "score": pred_values,
    })

    df["date_str"] = df["datetime"].dt.strftime("%Y%m%d")

    print("Exporting daily CSVs to:", args.output_dir)
    grouped = df.groupby("date_str")
    count = 0
    for date_str, g in grouped:
        out_path = os.path.join(args.output_dir, f"prediction_{date_str}.csv")
        # Only keep code and score columns
        g_out = g[["code", "score"]].sort_values("score", ascending=False)
        g_out.to_csv(out_path, index=False)
        count += 1

    print(f"Exported {count} daily files to {args.output_dir}")


if __name__ == "__main__":
    main()
