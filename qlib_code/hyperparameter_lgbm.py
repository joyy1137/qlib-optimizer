import qlib
import optuna
from qlib.constant import REG_CN
from qlib.utils import init_instance_by_config
from qlib.workflow.exp import Experiment
from qlib.tests.data import GetData
from qlib.workflow import R
import warnings
warnings.simplefilter("ignore", category=FutureWarning)
warnings.filterwarnings("ignore")
import logging
log = logging.getLogger(__name__)

def objective(trial, dataset):
    task = {
        "model": {
            "class": "LGBModel",
            "module_path": "qlib.contrib.model.gbdt",
            "kwargs": {
                "loss": "mse",
                "colsample_bytree": trial.suggest_uniform("colsample_bytree", 0.5, 1),
                "learning_rate": trial.suggest_uniform("learning_rate", 0, 1),
                "subsample": trial.suggest_uniform("subsample", 0, 1),
                "lambda_l1": trial.suggest_loguniform("lambda_l1", 1e-8, 1e4),
                "lambda_l2": trial.suggest_loguniform("lambda_l2", 1e-8, 1e4),
                "max_depth": 10,
                "num_leaves": trial.suggest_int("num_leaves", 1, 1024),
                "feature_fraction": trial.suggest_uniform("feature_fraction", 0.4, 1.0),
                "bagging_fraction": trial.suggest_uniform("bagging_fraction", 0.4, 1.0),
                "bagging_freq": trial.suggest_int("bagging_freq", 1, 7),
                "min_data_in_leaf": trial.suggest_int("min_data_in_leaf", 1, 50),
                "min_child_samples": trial.suggest_int("min_child_samples", 5, 100),
            },
        },
    }
    evals_result = dict()
    model = init_instance_by_config(task["model"])
    model.fit(dataset, evals_result=evals_result)
  
    return min(evals_result["valid"]["l2"])


if __name__ == "__main__":
    provider_uri = "E:\\qlib_data\\tushare_qlib_data\\qlib_bin"
    GetData().qlib_data(target_dir=provider_uri, region=REG_CN, exists_skip=True)
    qlib.init(provider_uri=provider_uri, region="cn")

    
    custom_dataset_config = {
        "class": "DatasetH",
        "module_path": "qlib.data.dataset",
        "kwargs": {
            "handler": {
                "class": "Alpha158",
                "module_path": "qlib.contrib.data.handler",
                "kwargs": {
                    "start_time": "2015-01-01",
                    "end_time": "2025-11-01",
                    "instruments": "all",
                },
            },
            "segments": {
                "train": ("2015-01-01", "2025-10-28"),
                "valid": ("2025-10-28", "2025-10-30"),
                "test": ("2025-10-30", "2025-11-01"),
            },
        },
    }
    dataset = init_instance_by_config(custom_dataset_config)

    study = optuna.Study(study_name="LGBM_158", storage="sqlite:///db.sqlite3")
 
    # R.start(experiment_name="lgbm_optuna", recorder_name="run_1")
    study.optimize(lambda trial: objective(trial, dataset), n_trials=60, n_jobs=1)

   
    if len(study.trials) > 0 and getattr(study, "best_trial", None) is not None:
        log.info("Best params:")
        log.info(study.best_params)
    else:
        log.error("No successful trial found.")


# optuna create-study --study LGBM_158 --storage sqlite:///db.sqlite3
# optuna-dashboard --port 5000 --host 0.0.0.0 sqlite:///db.sqlite3
