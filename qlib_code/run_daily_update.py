from __future__ import annotations
import argparse
import subprocess
import sys
import os
from pathlib import Path

WORKDIR = Path(__file__).resolve().parent


def run_cmd(cmd, cwd=None, env=None):
    print("\n> ", " ".join(cmd))
    res = subprocess.run(cmd, cwd=cwd or WORKDIR, env=env or os.environ, shell=False)
    if res.returncode != 0:
        raise SystemExit(f"Command failed (exit {res.returncode}): {' '.join(cmd)}")


def main():
    parser = argparse.ArgumentParser(description="Run full daily update pipeline")
    parser.add_argument("--date", help="目标预测日期 YYYY-MM-DD (可选)")
    parser.add_argument("--data_csv_dir", default=r"E:\\qlib_data\\tushare_qlib_data\\csv_data")
    parser.add_argument("--qlib_bin_dir", default=r"E:\\qlib_data\\tushare_qlib_data\\qlib_bin")
    parser.add_argument("--qlib_workdir", default=r"E:\\qlib")
    parser.add_argument("--include_fields", default="open,close,high,low,volume,factor,money")
    parser.add_argument("--dump_script", default=str(WORKDIR / "scripts" / "dump_bin.py"))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    py = sys.executable

    tushare_script = WORKDIR / "tushare2csv.py"
    if not tushare_script.exists():
        raise SystemExit(f"Missing script: {tushare_script}")

    print("获取最新数据...")
    # run_cmd([py, str(tushare_script)])

    dump_script = Path(args.dump_script)
    # If the provided dump_script doesn't exist, try qlib_workdir/scripts/dump_bin.py
    if not dump_script.exists():
        alt = Path(args.qlib_workdir) / "scripts" / "dump_bin.py"
        if alt.exists():
            dump_script = alt
        else:
            raise SystemExit(f"dump script not found at {dump_script} or {alt}. Please adjust --dump_script or ensure dump_bin.py exists in your qlib workdir.")

    print("转换数据格式...")
    dump_cmd = [py, str(dump_script), "dump_update", "--data_path", args.data_csv_dir,
                "--qlib_dir", args.qlib_bin_dir,
                "--include_fields", args.include_fields]
    
   
    qlib_workdir = args.qlib_workdir or r"E:\\qlib"
    if not Path(qlib_workdir).exists():
        print(f"Warning: qlib_workdir {qlib_workdir} does not exist. Attempting to run anyway.")
    run_cmd(dump_cmd, cwd=str(qlib_workdir))

    update_script = WORKDIR / "update_new.py"
    if not update_script.exists():
        raise SystemExit(f"Missing script: {update_script}")

    print("预测分数...")
    env = os.environ.copy()
    if getattr(args, "date", None):
        env["TARGET_PREDICT_DATE"] = args.date
    if getattr(args, "global_tools_path", None):
        env["GLOBAL_TOOLSFUNC_test"] = args.global_tools_path

    env["QLIB_PROVIDER_URI"] = args.qlib_bin_dir

    run_cmd([py, str(update_script)], env=env)

    print("\n每日更新完成")


if __name__ == "__main__":
    main()
