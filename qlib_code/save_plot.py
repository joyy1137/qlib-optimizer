import shap
from matplotlib import pyplot as plt
from qlib.workflow import R
import qlib
from qlib.config import REG_CN
from qlib.contrib.report import analysis_position, analysis_model
import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=UserWarning)

import shutil
import os

def save_all_figures(experiment_id, experiment_name,provider_uri, mlruns_uri, output_dir=r"E:\qlib_data\analysis_figures"):
    

    qlib.init(provider_uri=provider_uri, region=REG_CN)
    R.set_uri(mlruns_uri)
    recorder = R.get_recorder(experiment_id=experiment_id, experiment_name=experiment_name)

    report_normal_df = recorder.load_object("portfolio_analysis/report_normal_1day.pkl")
    positions = recorder.load_object("portfolio_analysis/positions_normal_1day.pkl")
    analysis_df = recorder.load_object("portfolio_analysis/port_analysis_1day.pkl")
    pred = recorder.load_object("pred.pkl")
    label = recorder.load_object("label.pkl")

    import pandas as pd
    pred_label = pd.concat([pred, label], axis=1, sort=True)
    pred_label.columns = ['score', 'label']
    fig_list = []

    try:
        figs1 = analysis_position.report_graph(report_normal_df, show_notebook=False)
        fig_list.extend(figs1)
    except Exception as e:
        print(f"报告图生成失败: {e}")

    try:
        figs2 = analysis_position.risk_analysis_graph(analysis_df, report_normal_df, show_notebook=False)
        fig_list.extend(figs2)
    except Exception as e:
        print(f"风险分析图生成失败: {e}")

    try:
        figs3 = analysis_position.score_ic_graph(pred_label, show_notebook=False)
        fig_list.extend(figs3)
    except Exception as e:
        print(f"IC图生成失败: {e}")

    try:
        figs5 = analysis_model.model_performance_graph(pred_label, show_notebook=False)
        fig_list.extend(figs5)
    except Exception as e:
        print(f"模型性能图生成失败: {e}")


    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    saved_paths = []
    failed_saves = []

    for i, fig in enumerate(fig_list):
        try:
            filename = f"figure_{i}.png"
            filepath = os.path.join(output_dir, filename)
            try:
                import kaleido
                # Prefer plotly's write_image when dealing with plotly figures.
                try:
                    # If this is a plotly figure, use plotly.io.write_image (uses kaleido)
                    import plotly.io as pio
                    # Some figures can be matplotlib figures; detect and handle below.
                    pio.write_image(fig, filepath, engine="kaleido")
                except Exception:
                    # Fall back to figure's own write_image or to_image methods if available.
                    if hasattr(fig, 'write_image'):
                        fig.write_image(filepath)
                    elif hasattr(fig, 'to_image'):
                        img_bytes = fig.to_image(format="png")
                        with open(filepath, "wb") as f:
                            f.write(img_bytes)
                    else:
                        raise
                saved_paths.append(filepath)
            except Exception as e1:
                try:
                    import matplotlib
                    matplotlib.use('Agg')
                    
                    # If it's a matplotlib figure, save directly.
                    if hasattr(fig, 'savefig'):
                        fig.savefig(filepath, bbox_inches='tight')
                        saved_paths.append(filepath)
                    # If it's a plotly figure but kaleido failed above, try to_image (may still use kaleido)
                    elif hasattr(fig, 'to_image'):
                        try:
                            img_bytes = fig.to_image(format="png")
                            with open(filepath, "wb") as f:
                                f.write(img_bytes)
                            saved_paths.append(filepath)
                        except Exception as e_toimg:
                            # As a last-resort fallback, write an HTML version so user can open it manually.
                            html_path = filepath.replace('.png', '.html')
                            try:
                                if hasattr(fig, 'write_html'):
                                    fig.write_html(html_path)
                                    print(f"Kaleido/png conversion failed; saved HTML fallback: {html_path}")
                                else:
                                    raise Exception("无法保存为 HTML 的 fallback")
                                saved_paths.append(html_path)
                            except Exception as e_html:
                                raise Exception(f"无法转换图形: {e_html}")
                    else:
                        raise Exception("无法转换图形: 未知图形类型")
                except Exception as e3:
                    # If kaleido produced browser subprocess errors, include that in the log and save html fallback when possible.
                    print(f"✗ 所有方法都失败: {e3}")
                    failed_saves.append(i)
        except Exception as e:
            print(f"✗ 保存图片 {i} 时发生错误: {e}")
            failed_saves.append(i)

    #==== SHAP图生成和保存 ====
    print("\n=== 生成并保存SHAP图 ===")
    try:
        
        for seg in ["valid", "test"]:
            shap_values = recorder.load_object(f"shap_values_{seg}.pkl")
            X = recorder.load_object(f"shap_X_{seg}.pkl")  
            # summary_plot
            plt.figure()
            shap.summary_plot(shap_values, X, show=False)
            plt.title(f"SHAP Summary ({seg})", fontsize=16)
            shap_fig_path = os.path.join(output_dir, f'shap_summary_{seg}.png')
            plt.savefig(shap_fig_path, bbox_inches='tight')
            plt.close()

            plt.figure()
            shap.summary_plot(shap_values, X, plot_type="bar", show=False)
            plt.title(f"SHAP Bar ({seg})", fontsize=16)
            shap_bar_fig_path = os.path.join(output_dir, f'shap_bar_{seg}.png')
            plt.savefig(shap_bar_fig_path, bbox_inches='tight')
            plt.close()

            saved_paths.append(shap_fig_path)
            saved_paths.append(shap_bar_fig_path)
            print(f"SHAP summary图已保存: {shap_fig_path}")
            print(f"SHAP bar图已保存: {shap_bar_fig_path}")

    except Exception as e:
        print(f"SHAP图生成或保存失败: {e}")

    # ==== 特征重要性图生成和保存 ====
    print("\n=== 生成并保存特征重要性图 ===")
    try:
        
        for seg in ["valid", "test"]:
            X = recorder.load_object(f"shap_X_{seg}.pkl") 
            feature_names = list(X.columns)

            # 获取所有 artifact 名称
            artifact_names = [os.path.basename(x) for x in recorder.list_artifacts()]
            # 获取模型对象
            if 'trained_model' in artifact_names:
                model = recorder.load_object('trained_model')
            else:
                print(f"未找到 trained_model，跳过 {seg} 的特征重要性图生成。")
                continue

            # Gain Importance
            try:
                importance_gain = recorder.load_object('gain_importance.pkl')
            except Exception:
                importance_gain = model.model.feature_importance(importance_type='gain')
            plt.figure(figsize=(10, 8))
            gain_df = pd.DataFrame({'feature': feature_names, 'gain_importance': importance_gain})
            gain_df = gain_df.sort_values('gain_importance', ascending=False).head(20)
            plt.barh(gain_df['feature'][::-1], gain_df['gain_importance'][::-1])
            plt.title(f'Gain Importance ({seg})', fontsize=16)
            plt.tight_layout()
            gain_fig_path = os.path.join(output_dir, f'gain_importance_{seg}.png')
            plt.savefig(gain_fig_path, bbox_inches='tight')
            plt.close()
            saved_paths.append(gain_fig_path)
            print(f"Gain Importance图已保存: {gain_fig_path}")

            # Split Importance
            if 'split_importance.pkl' in artifact_names:
                importance_split = recorder.load_object('split_importance.pkl')
            else:
                importance_split = model.model.feature_importance(importance_type='split')
            plt.figure(figsize=(10, 8))
            split_df = pd.DataFrame({'feature': feature_names, 'split_importance': importance_split})
            split_df = split_df.sort_values('split_importance', ascending=False).head(20)
            plt.barh(split_df['feature'][::-1], split_df['split_importance'][::-1])
            plt.title(f'Split Importance ({seg})', fontsize=16)
            plt.tight_layout()
            split_fig_path = os.path.join(output_dir, f'split_importance_{seg}.png')
            plt.savefig(split_fig_path, bbox_inches='tight')
            plt.close()
            saved_paths.append(split_fig_path)
            print(f"Split Importance图已保存: {split_fig_path}")


    except Exception as e:
        print(f"特征重要性图生成或保存失败: {e}")

    print(f"\n=== 保存结果 ===")
    print(f"成功保存: {len(saved_paths)} 个文件")
    print(f"保存失败: {len(failed_saves)} 个文件")

if __name__ == "__main__":
    output_dir=r"E:\qlib_data\analysis_figures"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    
    save_all_figures(
        experiment_id="505608931795866282",
        experiment_name="test_lgbm",
        provider_uri=r"E:\qlib_data\tushare_qlib_data\qlib_bin",
        mlruns_uri="file:///E:/qlib_code/mlruns",
        output_dir = output_dir
    )

