from __future__ import annotations
import argparse
import subprocess
import sys
import os
from pathlib import Path
import yaml
from typing import TextIO

WORKDIR = Path(__file__).resolve().parent
cfg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'paths.yaml'))
with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f) or {}
csv_path = cfg['csv_output_dir']
qlib_bin_dir = cfg['qlib_bin_dir']
qlib_workdir = Path(cfg['qlib_workdir'])


class Tee:
    """Duplicate writes to original stream and a file-like object.

    Writes are attempted to both the original stream (usually the console)
    and the logfile. flush() is proxied to both.
    """
    def __init__(self, orig: TextIO, logfile: TextIO) -> None:
        self.orig = orig
        self.logfile = logfile

    def write(self, data: str) -> None:
        try:
            self.orig.write(data)
        except Exception:
            pass
        try:
            self.logfile.write(data)
        except Exception:
            pass

    def flush(self) -> None:
        try:
            self.orig.flush()
        except Exception:
            pass
        try:
            self.logfile.flush()
        except Exception:
            pass


def run_cmd(cmd, cwd=None, env=None):
    # Print the command. Use map(str, ...) to support Path objects.
    print("\n> ", " ".join(map(str, cmd)))

    proc = subprocess.Popen(
        list(map(str, cmd)),
        cwd=str(cwd or WORKDIR),
        env=env or os.environ,
        shell=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        text=True,
        bufsize=1,
    )

    # Stream lines as they arrive. If the process never writes anything this
    # loop will simply wait until the child exits; that's the intended
    # behaviour for seeing live logs.
    try:
        if proc.stdout is not None:
            for line in proc.stdout:
                # Preserve original formatting
                print(line, end="")
    finally:
        # Ensure we reap the child process
        proc.wait()

    if proc.returncode != 0:
        raise SystemExit(f"Command failed (exit {proc.returncode}): {' '.join(map(str, cmd))}")


def main():
    parser = argparse.ArgumentParser(description="Run full daily update pipeline")
    parser.add_argument("--date", help="目标预测日期 YYYY-MM-DD (可选)")
    parser.add_argument("--data_csv_dir", default=csv_path)
    parser.add_argument("--qlib_bin_dir", default=qlib_bin_dir)
    parser.add_argument("--qlib_workdir", default=qlib_workdir)
    parser.add_argument("--include_fields", default="open,close,high,low,volume,factor,money")
    parser.add_argument("--dump_script", default=str(qlib_workdir/ "scripts" / "dump_bin.py"))
    parser.add_argument("--log-file", default=str(WORKDIR / "../logs/score_prediction.log"), help="Path to append log output")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    # Open log file and tee stdout/stderr so output is also saved locally.
    log_f = None
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    try:
        try:
            log_f = open(args.log_file, "a", encoding="utf-8")
        except Exception as e:
            print(f"Warning: cannot open log file {args.log_file} for append: {e}")
            log_f = None

        if log_f is not None:
            sys.stdout = Tee(original_stdout, log_f)
            sys.stderr = Tee(original_stderr, log_f)

        py = sys.executable
    except Exception:
        # If anything goes wrong early, restore streams and re-raise
        if log_f:
            try:
                log_f.close()
            except Exception:
                pass
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        raise

    tushare_script = WORKDIR / "tushare2csv.py"
    if not tushare_script.exists():
        raise SystemExit(f"Missing script: {tushare_script}")

    print("获取最新数据...")
    run_cmd([py, str(tushare_script)])

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

    # Restore stdout/stderr and close log file
    try:
        sys.stdout.flush()
    except Exception:
        pass
    try:
        sys.stderr.flush()
    except Exception:
        pass
    sys.stdout = original_stdout
    sys.stderr = original_stderr
    if log_f:
        try:
            log_f.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
