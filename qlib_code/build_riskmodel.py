import os
import pandas as pd
from datetime import datetime

def convert_risk_model_data():
    dir = r"E:\qlib_data\data_factor"
    source_to_target = {
        dir + r'\factor_exposure': r'factor_exp',
        dir + r'\FactorCov': r'factor_cov',
        dir + r'\FactorSpecificRisk': r'specific_risk'
    }

    target_root = r"E:\qlib_data\riskmodel"
    os.makedirs(target_root, exist_ok=True)
  

    for source_folder, target_prefix in source_to_target.items():
        
        files = [f for f in os.listdir(source_folder) if f.endswith(('csv'))]

        for csv_file in files:
            try:
             
                name = os.path.splitext(csv_file)[0]
                date_str = None
               
                import re
                m = re.search(r'(20\d{6})', name)
                if m:
                    date_str = m.group(1)
                else:
                    # use file mtime
                    src_path = os.path.join(source_folder, csv_file)
                    mtime = os.path.getmtime(src_path)
                    date_str = datetime.fromtimestamp(mtime).strftime('%Y%m%d')

                date_folder = os.path.join(target_root, date_str)
                os.makedirs(date_folder, exist_ok=True)

                file_path = os.path.join(source_folder, csv_file)
               
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, encoding='gbk')
               
                if target_prefix == 'factor_exp':
                    
                    if 'valuation_date' in df.columns:
                        df = df.drop('valuation_date', axis=1)
                    if 'code' in df.columns:
                        df = df.set_index('code')
                   
                    for col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    df = df.fillna(0)
                    
                elif target_prefix == 'factor_cov':
                    
                    if 'valuation_date' in df.columns:
                        df = df.drop('valuation_date', axis=1)
                    # 将第一列设置为索引（因子名称）
                    if len(df.columns) > 0:
                        df = df.set_index(df.columns[0])
                    # 确保所有值都是数值类型
                    df = df.apply(pd.to_numeric, errors='coerce')
                    df = df.fillna(0)
                    
                elif target_prefix == 'specific_risk':
                 
                    # 需要将code设置为索引
                    if 'valuation_date' in df.columns:
                        df = df.drop('valuation_date', axis=1)
                    if 'code' in df.columns:
                        df = df.set_index('code')
                   
                    for col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    df = df.fillna(0)

                target_file_base = os.path.join(date_folder, target_prefix)
                out_file = f"{target_file_base}.csv"
                df.to_csv(out_file)
               

            except Exception as e:
                print(f"处理 {csv_file} 时出错: {str(e)}")

    print("\n所有文件转换完成!")

if __name__ == "__main__":
    convert_risk_model_data()
