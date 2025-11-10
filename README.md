# Qlib Optimizer

这是一个用于量化策略训练、预测与优化的仓库，结合了 Qlib（用于数据和模型流程）和 Matlab 优化脚本，包含数据导入、模型训练/预测、以及将预测结果写入数据库的流程。

## 目录概览

# Qlib Optimizer

整合 Qlib（用于数据与模型流程）和 Matlab 优化/回测脚本的量化策略仓库。
这是一个面向「从数据准备、模型预测，到基于预测分数的组合优化」的端到端工具集，适合用来做日常预测流水线、超参数调优和 Matlab 层面的优化回测。

## 主要功能（简要）
- 从 CSV 或外部数据源（例如 Tushare）构建 Qlib 二进制数据并运行模型做单日预测。
- 使用 Matlab 脚本对预测分数做组合优化并回测（`Optimizer_matlab/`）。
- 提供示例的超参数搜索（Optuna）脚本与 qrun 配置，方便在 Qlib 上训练/调参。

## 仓库结构概览

- `qlib_code/` — Qlib 相关的 Python 脚本与工具：
  - `run_daily_update.py` — 日常流水线入口：拉取/转换数据（调用 qlib 的 dump_bin）、运行预测、导出 CSV、写入 DB。
  - `update_new.py` — 加载训练好的模型并对指定日期生成预测文件。
  - `sql2csv.py`、`importer.py` — 数据获取与导入工具。
  - `hyperparameter_lgbm.py` — Optuna 超参搜索示例（LightGBM）。
  - `import_weight_to_mysql.py` — 将导出的权重/回测结果导入 MySQL 的工具。

- `Optimizer_matlab/` — Matlab 优化与回测：
  - `run_optimizer.m`, `run_backtest.m`, `batch_run_optimizer.m` 等脚本
  - `BacktestToolbox/` — 回测工具集（多个辅助函数）
  - `config/` — Matlab 端的配置（数据库、项目参数等）

- `config/` — YAML 配置（项目根目录）：
  - `paths.yaml` — provider uri、模型路径、预测输出目录等
  - `db.yaml` — 数据库连接配置（用于 Python / Matlab 的导入器）

- `logs/` — 运行日志
  - `score_prediction.log` — 生成预测分数时候的log
  - `weight_optimizer.log` — 优化得到权重时候的log

- `db.sqlite3` — 可选的本地 sqlite 存储（用于 Optuna 等）
- `requirements.txt` — Python 依赖清单（用于创建虚拟环境）。

## 快速开始（Windows / PowerShell）

下面的步骤假设你在 Windows 上并使用 PowerShell；如果使用其它 shell，命令请相应调整。

1) 创建并激活 Python 虚拟环境（示例使用 Conda）：

```powershell
conda create -n qlib_env python=3.12 -y; conda activate qlib_env
```

2) 安装 Python 依赖：

```powershell
python -m pip install -r requirements.txt
```

3) 配置 `config/paths.yaml` 和 `config/db.yaml`：
 
4) 准备 qlib（用于将 CSV 转成 qlib 的二进制数据）
   - 仓库不会内置 qlib 的 `dump_bin.py`，请按需 clone 官方 qlib：

```powershell
git clone https://github.com/microsoft/qlib.git
# 然后确保在 PATH 或脚本中能找到 qlib 的工具
```

## 运行日常预测流水线

示例：在仓库根目录并激活虚拟环境后运行（默认预测当日数据）：

```powershell
python .\main.py
```

或直接指定日期生成分数，运行 `run_daily_update.py`：

```powershell
python .\qlib_code\run_daily_update.py --date 2025-10-27
```

该流程通常会：
- 拉取或读取最新 CSV 数据；
- 调用 qlib 的 `dump_bin.py` 将 CSV 转为 qlib 格式（需提前准备 qlib）；
- 加载已训练的模型并运行 `update_new.py` 以产出 `prediction_YYYYMMDD.csv`；
- 根据 `config/db.yaml` 将结果写入数据库；
- 调用 Matlab 优化器（可选）生成并导出权重数据。

输出位置：`config/paths.yaml` 中的 `prediction_output_dir`（默认为仓库内某目录，请检查配置）。

## 使用 Matlab 优化与回测（可选）

`Optimizer_matlab/` 内包含基于预测分数的优化与回测脚本。典型流程：

- 将生成的 `prediction_YYYYMMDD.csv` 提供给 Matlab 脚本作为 score 源（或从数据库中读取）；
- 运行 ``run_optimizer.m` 来生成组合权重；

注意：默认 Matlab 的一些脚本使用 `ConfigReaderToday`（将只处理最新日期）。如果需要生成历史日期的权重，需：

- 将 `Optimizer_matlab/merge_portfolio_dataframe.m`, `data_preparation.m`, `batch_run_optimizer.m` 中的 `ConfigReaderToday` 改为 `ConfigReader`；
- 在 `Optimizer_matlab/config/config_db.m` 中将 `db_config.score_source = 'csv'` 改为 `db`（如果你希望从数据库读取 score）。

也可以使用 `ConfigReaderToday(specifiedDate=...)` 的方式指定具体日期运行。

## 模型训练与超参数调优（Optuna + qrun）

1) 使用 Optuna 创建 study（示例使用本地 sqlite）：

```powershell
optuna create-study --study LGBM_158 --storage sqlite:///db.sqlite3
optuna-dashboard --port 5000 --host 0.0.0.0 sqlite:///db.sqlite3
```

2) 在另一个终端运行超参搜索脚本：

```powershell
python .\qlib_code\hyperparameter_lgbm.py
```

3) 将找到的最优参数写入你的 qrun 配置文件（例如 `workflow_config_lightgbm.yaml`），然后执行 qrun：

```powershell
qrun workflow_config_lightgbm.yaml
```

注意：运行 `qrun` 时，请在工作目录中正确放置或引用配置文件路径。

## 常见问题与排查建议

- 如果 `run_daily_update.py` 未能找到 qlib 的 `dump_bin.py`：请确认已 clone qlib 并将其路径加入环境或在脚本中使用绝对路径调用。
- 数据库写入失败：检查 `config/db.yaml` 的连接信息，并确保目标数据库可访问且表权限正确。
- Matlab 运行问题：确认 MATLAB 的路径和依赖（如 `yamlmatlab`）已安装，并且 Matlab 脚本中引用的配置文件路径正确。


