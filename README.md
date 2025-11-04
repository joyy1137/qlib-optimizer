# Qlib Optimizer

这是一个用于量化策略训练、预测与优化的仓库，结合了 Qlib（用于数据和模型流程）和 Matlab 优化脚本，包含数据导入、模型训练/预测、以及将预测结果写入数据库的流程。

## 目录概览

- `qlib_code/` - Python 代码与 Qlib 相关的脚本：
  - `update_new.py` - 加载已训练模型并对指定的交易日进行预测，生成 CSV 并写入 MySQL（或配置的数据库）。
  - `run_daily_update.py`, `importer.py`, `tushare2csv.py` 等 - 辅助脚本，用于数据更新、导入与处理。
  - `requirements.txt` - Python 依赖（用于创建虚拟环境）。
  - 其他脚本用于导出权重、绘图、模型解释（shap）等。
- `Optimizer_matlab/` - Matlab 优化与回测工具：
  - `run_optimizer.m`, `run_backtest.m`, `batch_run_optimizer.m` 等，包含一个 `BacktestToolbox/` 子文件夹。
- `config/` - YAML 配置文件（根目录下）：
  - `paths.yaml` - 路径与资源配置（provider uri、model 存放路径、预测输出目录等）。
  - `db.yaml` - 数据库连接配置（用于 `MySQLImporter`）。
- 其它文件：Jupyter 笔记本（`aa.ipynb`）等。

## 项目功能（简述）

- 使用 Qlib 的数据 handler 准备数据集并在指定 `TARGET_PREDICT_DATE` 上运行模型预测。
- 将预测结果保存为 CSV 文件（按 `prediction_YYYYMMDD.csv` 命名）并写入数据库表。
- Matlab 部分用于基于预测分数做组合优化。


## 快速设置（Python）

1. 克隆仓库并进入目录（已由你在本地完成）。
2. 建议创建并激活虚拟环境（示例使用 Conda）：

```powershell
# 创建并激活 conda 环境（示例）
conda create -n qlib_env python=3.9 -y; conda activate qlib_env
# 或使用 venv
python -m venv .venv; .\.venv\Scripts\Activate.ps1
```

3. 安装 Python 依赖：

```powershell
pip install -r qlib_code\requirements.txt
```

4. 编辑 `config\paths.yaml` 和 `config\db.yaml`：
   - `paths.yaml` 应包含 `provider_uri`, `model_path`, `prediction_output_dir` 等字段。
   - `db.yaml` 为数据库连接配置，供 `MySQLImporter` 使用。


## 运行 Qlib 预测脚本
- 使用 `run_daily_update.py` 获取每日预测

  `run_daily_update.py`（位于 `qlib_code/`）是日常更新与预测的入口脚本。通常你可以在已激活的 Python 环境中直接运行它来完成当天的数据更新、模型预测并将结果保存到 CSV 与数据库。

  推荐的 PowerShell 快速运行示例（假设当前目录为仓库根目录，并已激活虚拟环境）：

  # 运行日常更新与预测
  python .\qlib_code\run_daily_update.py
  ```

  运行后会触发数据更新（如果脚本实现了该逻辑），并执行模型预测流程，最后在 `config/paths.yaml` 指定的 `prediction_output_dir` 中产出 `prediction_YYYYMMDD.csv`，同时将结果写入数据库（通过 `MySQLImporter`）。

- 仅运行预测（可选）

  如果你只想执行预测（不执行其他日常数据更新），可以直接运行 `update_new.py`：

  ```powershell
  # 可选：确保 global_tools 可导入
  $env:GLOBAL_TOOLSFUNC_test = 'C:\path\to\your\tools'
  python .\qlib_code\update_new.py
  ```

  `update_new.py` 会根据当前日期与 `global_tools.is_workday()` 的结果来决定 `TARGET_PREDICT_DATE`：
  - 如果当天不是交易日，脚本会选取上一个交易日作为预测日期；
  - 如果当天是交易日但时间早于 19:00，则也会回退到上一个交易日；
  - 否则以当天为预测日期。

  注意：脚本当前不会从环境变量读取 `TARGET_PREDICT_DATE`，若需要强制某个日期进行预测，请在脚本中临时修改或创建一个小的 wrapper 脚本将所需日期传入并调用 `update_new.main()`。

  常见问题：
  - 如果脚本找不到 `global_tools`，请确认 `GLOBAL_TOOLSFUNC_test` 指向包含 `global_tools.py` 的目录。
  - 若模型无法加载（pickle 错误），请确认 `config/paths.yaml` 中 `model_path` 指向兼容当前环境的模型文件。


## 运行 Matlab 优化与回测

- 在 `Optimizer_matlab/` 文件夹中有若干 Matlab 脚本：
  - 在 MATLAB 环境中打开并运行 `run_optimizer.m` 或 `batch_run_optimizer.m` 来依次进行优化/回测。


## 输出

- 预测 CSV：由 `update_new.py` 输出到 `prediction_output_dir`（见 `paths.yaml`），文件名形如 `prediction_YYYYMMDD.csv`。
- 数据库：预测会写入到指定表（通过 `MySQLImporter.df_to_mysql`），表结构在脚本中有示例 schema（`valuation_date`, `code`, `final_score`, `score_name`, `update_time`）。

## 常见问题与排查建议

- 模型加载失败（pickle）：请确认 `paths.yaml` 中 `model_path` 指向一个存在且兼容当前 Python 环境与库版本的 pickle 文件。
- Qlib provider 连接失败：确认 `provider_uri` 在 `paths.yaml` 配置正确并且数据已初始化（qlib.init 成功）。
- `global_tools` 导入失败：检查 `GLOBAL_TOOLSFUNC_test` 环境变量是否正确设置，且路径中包含 `global_tools.py`。
- 数据库写入失败：检查 `db.yaml` 配置、数据库是否允许外部连接、目标表权限与字符集（MySQL）等。

