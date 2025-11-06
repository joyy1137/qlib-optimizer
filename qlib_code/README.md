# 项目概览

这是一个基于 Qlib 的量化研究/预测仓库，包含数据获取、数据转换、模型调参与模型预测的脚本集合。主要功能包括：

- 从 Tushare 拉取日线（后复权）与指数数据并保存为 CSV（`tushare2csv.py`）。
- 将 CSV 转换为 Qlib 二进制数据（通过 qlib 的 `dump_bin.py`，由 `run_daily_update.py` 调用）。
- 加载训练好的模型并对单日进行预测（`update_new.py`）。
- 超参数搜索示例（`hyperparameter_lgbm.py` 使用 Optuna）。

目录结构（工作区顶层文件示例）：

- `tushare2csv.py`  => 将 Tushare 数据拉取并写入 CSV。
- `run_daily_update.py` => 运行日常流水线（拉数据 -> 转格式 -> 预测）。
- `update_new.py` => 加载模型并对目标日做预测，输出 `prediction_YYYYMMDD.csv`。
- `hyperparameter_lgbm.py` => 用 Optuna 对 LGBM 模型做超参搜索的脚本。
- `import_weight_to_mysql.py` - 将导出的组合权重或回测结果导入 MySQL 的工具脚本，用于把优化结果写入数据库以便下游使用。


## 快速开始（Windows / PowerShell）

1. 创建 Python 虚拟环境并激活（示例使用 conda）：

```powershell
conda create -n qlib_env python=3.12 -y; conda activate qlib_env
```

2. 安装依赖（见 `requirements.txt`）：

```powershell
python -m pip install -r requirements.txt
```

3. 运行日常流水线（示例）：

```powershell
python run_daily_update.py --date 2025-10-27 (默认预测当日数据)
```

该命令会：拉取最新 CSV -> 调用 qlib 的 `dump_bin.py`（需要先git clone https://github.com/microsoft/qlib.git）-> 运行 `update_new.py` 生成预测文件。

## 模型训练（先调参再运行 qrun）

1. 首先在第一个终端运行以下代码去创建一个数据库，并且记录调参过程中的数据：
```
optuna create-study --study LGBM_158 --storage sqlite:///db.sqlite3
optuna-dashboard --port 5000 --host 0.0.0.0 sqlite:///db.sqlite3
```
2. 在第二个终端中运行以下调参脚本：
```
python hyperparameter_158.py
```
调参完成后会打印出最佳参数，或去数据库查看

3. 传入上一步获得的超参数到模型配置文件，运行
```
qrun workflow_config_lightgbm.yaml
```
需要注意切换到配置文件所在路径

