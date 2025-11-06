# Qlib Optimizer

这是一个用于量化策略训练、预测与优化的仓库，结合了 Qlib（用于数据和模型流程）和 Matlab 优化脚本，包含数据导入、模型训练/预测、以及将预测结果写入数据库的流程。

## 目录概览

- `qlib_code/` - Python 代码与 Qlib 相关的脚本：
  - `update_new.py` - 加载已训练模型并对指定的交易日进行预测，生成 CSV 并写入 MySQL（或配置的数据库）。
  - `run_daily_update.py`, `importer.py`, `tushare2csv.py` 等 - 辅助脚本，用于数据更新、导入与处理。
  - `import_weight_to_mysql.py` - 将导出的权重/组合信息导入 MySQL 的工具脚本。
  - `requirements.txt` - Python 依赖（用于创建虚拟环境）。
  - 其他脚本用于导出权重、绘图、模型解释（shap）等。
- `Optimizer_matlab/` - Matlab 优化与回测工具：
  - `run_optimizer.m`, `run_backtest.m`, `batch_run_optimizer.m` 等，包含一个 `BacktestToolbox/` 子文件夹。
- `config/` - YAML 配置文件（根目录下）：
  - `paths.yaml` - 路径与资源配置（provider uri、model 存放路径、预测输出目录等）。
  - `db.yaml` - 数据库连接配置（用于 `MySQLImporter`）。


## 项目功能（简述）

- 使用 Qlib 的数据 handler 准备数据集并在指定 `TARGET_PREDICT_DATE` 上运行模型预测。
- 将预测结果保存为 CSV 文件（按 `prediction_YYYYMMDD.csv` 命名）并写入数据库表。
- Matlab 部分用于基于预测分数做组合优化。


## 快速设置（Python）

1. 克隆仓库并进入目录。
2. 建议创建并激活虚拟环境（示例使用 Conda）：
3. 需要确保已经下载yamlmatlab

```powershell
# 创建并激活 conda 环境（示例）
conda create -n qlib_env python=3.9 -y; conda activate qlib_env
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

## 运行日常更新与预测
  ```
  python .\main.py
  ```
  - 运行后会触发数据更新，执行模型预测流程，并且使用优化器得到最终权重。最后在 `config/paths.yaml` 指定的 `prediction_output_dir` 中产出预测得到的score `prediction_YYYYMMDD.csv`，同时根据`config/db.yaml`将结果写入数据库 `database.table_name `。并且会导出优化后的权重数据写入数据库 `database2.table_name2 `中。
  - 默认优化生成最新日期的权重。如需优化得到历史数据权重，需要将`.\Optimizer_matlab\merge_portfolio_dataframe.m`, `.\Optimizer_matlab\data_preparation.m`以及`.\Optimizer_matlab\batch_run_optimizer.m`中使用`ConfigReaderToday`的地方改成`ConfigReader`，并且将`.\Optimizer_matlab\config\config_db.m`中的`db_config.score_source = 'csv'`修改成`db_config.score_source = 'db'`
  
  - 如需生成特定日期的权重，可以修改上述使用`ConfigReaderToday`的地方，修改为`ConfigReaderToday(specifiedDate=指定日期)`。或者，在使用`ConfigReader`的情况下，直接修改`.\Optimizer_matlab\config\opt_project_config.m`中portfolio的开始和结束日期。

