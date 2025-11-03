from qlib.workflow.record_temp import SignalRecord
import shap
import logging

log = logging.getLogger(__name__)

class ShapRecord(SignalRecord):
    def __init__(self, model=None, dataset=None, **kwargs):
        super().__init__(model=model, dataset=dataset, **kwargs)

    def generate(self):
        recorder = self.recorder
        model = self.model
        explainer = shap.TreeExplainer(model.model)
  
        train_X = self.dataset.prepare('train')
        train_X = train_X[[col for col in train_X.columns if not str(col).lower().startswith('label')]]
        feature_names = list(train_X.columns)
        for seg in ["train", "valid", "test"]:
            try:
                X = self.dataset.prepare(seg)
        
                X = X[[col for col in X.columns if not str(col).lower().startswith('label')]]
                # 检查特征顺序和名称是否一致
                if list(X.columns) == list(feature_names):
                    X = X[feature_names]
                    shap_values = explainer.shap_values(X)
                    recorder.save_objects(**{f"shap_values_{seg}.pkl": shap_values})
                    recorder.save_objects(**{f"shap_X_{seg}.pkl": X})
                    log.info(f"SHAP values and X for {seg} saved.")
                else:
                    log.warning("特征顺序或名称不一致，差异如下：")
                    for i, (a, b) in enumerate(zip(X.columns, feature_names)):
                        if a != b:
                            log.warning(f"位置{i}: 数据特征={a}, 模型特征={b}")
                    log.warning("请检查特征工程，确保训练和推理特征顺序一致！已跳过SHAP计算。")
                    continue
            except Exception as e:
                
                log.exception(f"SHAP {seg} 生成或保存失败: {e}")
             
        
        # 保存 gain/split importance
        try:
            importance_gain = model.model.feature_importance(importance_type='gain')
            importance_split = model.model.feature_importance(importance_type='split')
            recorder.save_objects(
                gain_importance=importance_gain,
                split_importance=importance_split
            )
        except Exception as e:
            log.exception(f"保存 gain/split importance 失败: {e}")

        # 保存 trained_model
        try:
            recorder.save_objects(trained_model=model)
            
        except Exception as e:
            log.exception(f"保存 trained_model 失败: {e}")

        
