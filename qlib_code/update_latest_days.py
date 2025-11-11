import pandas as pd
import os
import time
import logging
import concurrent.futures
from datetime import datetime, timedelta, date
import yaml
import sqlalchemy
from sqlalchemy import create_engine, text
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

class QlibDataConverter:
   
    def __init__(self, output_dir):
       
      
        today_folder = date.today().strftime('%Y%m%d')
        self.csv_output_dir = os.path.join(output_dir, today_folder)
        db_config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'db.yaml'))
        with open(db_config_path, 'r', encoding='utf-8') as f:
            db_cfg = yaml.safe_load(f) or {}
        
        # 创建数据库连接
        try:
            db_url = f"mysql+pymysql://{db_cfg['user']}:{db_cfg['password']}@{db_cfg['host2']}:{db_cfg['port']}/{db_cfg['database3']}"
            self.engine = create_engine(db_url)
            log.info("数据库连接成功")
        except Exception as e:
            log.error(f"数据库连接失败: {e}")
            self.engine = None

        # 确保按今日日期创建输出目录
        os.makedirs(self.csv_output_dir, exist_ok=True)
        
    def get_latest_trading_date(self):
        """
        获取数据库中最新的交易日期
        """
        if self.engine is None:
            log.error("数据库未连接")
            return None
            
        try:
            query = text("""
                SELECT MAX(valuation_date) as latest_date 
                FROM data_stock 
                WHERE valuation_date <= CURDATE()
            """)
            
            result = pd.read_sql(query, self.engine)
            latest_date = result.iloc[0]['latest_date']
            
            if latest_date is not None:
                # 处理日期格式：如果已经是字符串，直接返回；如果是datetime对象，则格式化
                if isinstance(latest_date, str):
                    # 如果是字符串，尝试转换为标准格式
                    try:
                        # 尝试解析各种可能的日期格式
                        if '-' in latest_date:
                            # 已经是 YYYY-MM-DD 格式
                            latest_date_str = latest_date.replace('-', '')
                        else:
                            # 尝试解析其他格式
                            parsed_date = datetime.strptime(latest_date, '%Y%m%d')
                            latest_date_str = latest_date
                    except:
                        # 如果解析失败，尝试转换为datetime再格式化
                        try:
                            parsed_date = pd.to_datetime(latest_date)
                            latest_date_str = parsed_date.strftime('%Y%m%d')
                        except:
                            log.error(f"无法解析日期格式: {latest_date}")
                            return None
                else:
                    # 如果是datetime对象，直接格式化
                    latest_date_str = latest_date.strftime('%Y%m%d')
                
                log.info(f"获取到最新交易日期: {latest_date_str}")
                return latest_date_str
            else:
                log.warning("未找到最新交易日期")
                return None
                
        except Exception as e:
            log.error(f"获取最新交易日期失败: {e}")
            return None

    def get_hfq_data_from_sql(self, code, target_date):
        """
        从SQL数据库获取指定日期的复权后数据
        """
        if self.engine is None:
            log.error("数据库未连接")
            return None
            
        try:
            # 转换日期格式：确保是 YYYY-MM-DD 格式用于SQL查询
            if len(target_date) == 8 and target_date.isdigit():
                # 如果是 YYYYMMDD 格式，转换为 YYYY-MM-DD
                target_date_str = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
            else:
                target_date_str = target_date  # 假设已经是正确格式
            
            # 查询股票数据
            query = text("""
                SELECT valuation_date, code, open, high, low, close, volume, amt, adjfactor_jy 
                FROM data_stock 
                WHERE code = :code 
                AND valuation_date = :target_date
            """)
            
            df = pd.read_sql(query, self.engine, 
                           params={'code': code, 'target_date': target_date_str})
            
            if df is not None and not df.empty:
                log.info(f"从数据库获取 {code} 在 {target_date} 的数据成功")
                return df
            else:
                log.info(f"股票 {code} 在 {target_date} 无数据")
                return None
                
        except Exception as e:
            log.error(f"从数据库获取 {code} 在 {target_date} 的数据失败: {e}")
            return None

    def get_hfq_data_batch(self, codes, target_date):
        """
        批量获取指定日期的股票数据
        """
        if not codes:
            return None
        try:
            # 转换日期格式
            if len(target_date) == 8 and target_date.isdigit():
                target_date_str = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
            else:
                target_date_str = target_date
            
            codes_str = ','.join([f"'{code}'" for code in codes])
            
            query = text(f"""
                SELECT valuation_date, code, open, high, low, close, volume, amt, adjfactor_jy 
                FROM data_stock 
                WHERE code IN ({codes_str})
                AND valuation_date = :target_date
                ORDER BY code
            """)
            
            df = pd.read_sql(query, self.engine, 
                           params={'target_date': target_date_str})
            
            if df is not None and not df.empty:
                log.info(f"批量获取 {len(codes)} 只股票在 {target_date} 的数据成功")
                return df
            else:
                log.warning(f"在 {target_date} 批量查询返回空数据")
                return None
                
        except Exception as e:
            log.error(f"批量获取 {target_date} 数据失败: {e}，回退到逐只获取")
            return self._fallback_to_individual_queries(codes, target_date)
        
    
    def _fallback_to_individual_queries(self, codes, target_date):
        """
        回退到逐只股票查询
        """
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(codes))) as executor:
            future_to_code = {executor.submit(self.get_hfq_data_from_sql, code, target_date): code 
                            for code in codes}
            
            for future in concurrent.futures.as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    result = future.result()
                    if result is not None and not result.empty:
                        results.append(result)
                except Exception as e:
                    log.error(f"查询 {code} 在 {target_date} 失败: {e}")
        
        if not results:
            return None
            
        return pd.concat(results, ignore_index=True)
    
    def format_for_qlib(self, df, code=None):
        """
        格式化为Qlib需要的格式
        """
        if df is None or df.empty:
            return None
       
        # 保持与原来Tushare相同的列名映射
        column_mapping = {
            'valuation_date': 'date',
            'open': 'open',      
            'close': 'close', 
            'high': 'high',
            'low': 'low',
            'volume': 'volume',
            'amt': 'money',
            'adjfactor_jy': 'factor',
        }
     
        df = df.rename(columns=column_mapping)
        
        # 处理日期格式
        if 'date' in df.columns:
            # 如果日期列是字符串，先转换为datetime再格式化
            if df['date'].dtype == 'object':
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            else:
                df['date'] = df['date'].dt.strftime('%Y-%m-%d')
        
        required_columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'factor']
        if 'money' in df.columns:
            required_columns.append('money')
        
        # 只保留存在的列
        existing_columns = [col for col in required_columns if col in df.columns]
        df = df[existing_columns]
        return df
    
    def process_stock(self, code, target_date=None):
        """
        处理单只股票：只获取指定日期的数据
        """
        csv_path = os.path.join(self.csv_output_dir, f"{code}.csv")

        # 如果没有指定日期，使用最新交易日期
        if target_date is None:
            target_date = self.get_latest_trading_date()
            if target_date is None:
                log.error("无法获取最新交易日期")
                return False

        # 从数据库获取数据
        df = self.get_hfq_data_from_sql(code, target_date)
        if df is None or df.empty:
            log.info(f"股票 {code} 在 {target_date} 无数据，跳过")
            return False

        df_qlib = self.format_for_qlib(df, code)
        if df_qlib is None:
            return False

        return self._save_to_csv(df_qlib, csv_path)

    def _save_to_csv(self, df, csv_path):
        """
        保存数据到CSV文件
        """
        try:
            if os.path.exists(csv_path):
                existing = pd.read_csv(csv_path, parse_dates=['date'])
                frames = []
                for frame in (existing, df):
                    if frame is None:
                        continue
                    # skip truly empty DataFrames
                    if getattr(frame, 'empty', False):
                        continue
                    # drop columns that are all NA; if no columns remain, treat as empty
                    frame_clean = frame.dropna(axis=1, how='all')
                    # 如果过滤后没有列剩余，或者DataFrame为空，则跳过
                    if frame_clean.shape[1] == 0 or frame_clean.empty:
                        continue
                    frames.append(frame_clean) 

                # If nothing meaningful to concat, keep existing file untouched
                if not frames:
                    return True

                common_cols = set(frames[0].columns)
                for frame in frames[1:]:
                    common_cols = common_cols.intersection(frame.columns)
                
                # 只保留共有的列
                frames = [frame[list(common_cols)] for frame in frames]
                
                combined = pd.concat(frames, ignore_index=True)
                combined['date'] = pd.to_datetime(combined['date']).dt.strftime('%Y-%m-%d')
                combined.drop_duplicates(subset=['date'], keep='last', inplace=True)
                combined.sort_values('date', inplace=True)
                combined.to_csv(csv_path, index=False)
            else:
                # 确保操作的是副本而不是视图
                df_clean = df.dropna(axis=1, how='all').copy()  # 添加copy()
                if not df_clean.empty:
                    df_clean.sort_values('date', inplace=True)
                    df_clean.to_csv(csv_path, index=False)
            return True
        except Exception as e:
            log.error(f"写入CSV失败 ({csv_path}): {e}")
            return False

    def get_index_data(self, index_code, target_date=None):
        """
        从数据库获取指定日期的指数数据
        """
        if self.engine is None:
            return None
            
        # 如果没有指定日期，使用最新交易日期
        if target_date is None:
            target_date = self.get_latest_trading_date()
            if target_date is None:
                log.error("无法获取最新交易日期")
                return None
                
        try:
            # 转换日期格式
            if len(target_date) == 8 and target_date.isdigit():
                target_date_str = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
            else:
                target_date_str = target_date
            
            # 查询指数数据
            query = text("""
                SELECT valuation_date, code, open, high, low, close, volume, amt 
                FROM data_index 
                WHERE code = :index_code 
                AND valuation_date = :target_date
            """)
            
            df = pd.read_sql(query, self.engine, 
                           params={'index_code': index_code, 'target_date': target_date_str})
            
            if df is None or df.empty:
                log.warning(f"指数 {index_code} 在 {target_date} 无数据")
                return None

            # 指数数据没有复权因子，设为1.0
            df['factor'] = 1.0

            # 保持与原来相同的列名映射
            column_mapping = {
                'valuation_date': 'date',
                'open': 'open',      
                'close': 'close', 
                'high': 'high',
                'low': 'low',
                'volume': 'volume',
                'amt': 'money',
                'factor': 'factor',
            }
            df = df.rename(columns=column_mapping)
            
            # 处理日期格式
            if 'date' in df.columns:
                if df['date'].dtype == 'object':
                    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                else:
                    df['date'] = df['date'].dt.strftime('%Y-%m-%d')

            required_columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'factor']
            if 'money' in df.columns:
                required_columns.append('money')

            existing_columns = [col for col in required_columns if col in df.columns]
            df = df[existing_columns]
            return df
        except Exception as e:
            log.exception(f"获取指数 {index_code} 在 {target_date} 的数据失败: {e}")
            return None

    def process_index(self, index_code, target_date=None):
        """处理单个指数数据"""
        csv_path = os.path.join(self.csv_output_dir, f"{index_code}.csv")
        df = self.get_index_data(index_code, target_date)
        if df is None or df.empty:
            return False

        return self._save_to_csv(df, csv_path)

    def process_all_indices(self, market='ALL', target_date=None):
        """处理所有指数"""
        mapping = {
            'zz500': ['000905.SH'],
            'hs300': ['000300.SH'],
            'sz50': ['000016.SH'],
            'zz1000': ['000852.SH'],
            'zz2000': ['932000.CSI'],
            'ALL': ['000905.SH', '000300.SH', '000016.SH', '000852.SH', '932000.CSI'],
        }

        if isinstance(market, (list, tuple)):
            index_codes = market
        else:
            index_codes = mapping.get(market, [market])

        # 如果没有指定日期，使用最新交易日期
        if target_date is None:
            target_date = self.get_latest_trading_date()
            if target_date is None:
                log.error("无法获取最新交易日期")
                return

        for idx in index_codes:
            try:
                if self.process_index(idx, target_date=target_date):
                    log.info(f"处理指数 {idx} 在 {target_date} 成功")
                else:
                    log.warning(f"指数 {idx} 在 {target_date} 无数据")
            except Exception:
                log.exception(f"处理指数 {idx} 在 {target_date} 失败")
    
    def get_all_stocks(self, market='ALL'):
        """
        从数据库获取股票列表
        """
        log.info(f"正在获取 {market} 股票列表...")
        
        if self.engine is None:
            log.error("数据库未连接")
            return []
            
        try:
            if market in ['zz500', 'hs300', 'sz50','zz1000','zz2000']:
                query = text("""
                    SELECT DISTINCT code 
                    FROM data.indexcomponent 
                    WHERE organization = :market
                    ORDER BY code
                """)
                df = pd.read_sql(query, self.engine, params={'market': market})
            else:
                # 获取所有股票
                query = text("SELECT DISTINCT code FROM data_stock ORDER BY code")
                df = pd.read_sql(query, self.engine)
            
            stock_list = df['code'].tolist()
            log.info(f"获取到 {len(stock_list)} 只股票")
            return stock_list
            
        except Exception as e:
            log.error(f"获取股票列表失败: {e}")
            return []

    def process_all_stocks(self, market='ALL', target_date=None, batch_size=10):
        """
        处理全部股票的最新一天数据
        """
        # 如果没有指定日期，使用最新交易日期
        if target_date is None:
            target_date = self.get_latest_trading_date()
            if target_date is None:
                log.error("无法获取最新交易日期")
                return

        stock_list = self.get_all_stocks(market)
        if not stock_list:
            log.error("未获取到股票列表")
            return

        success_count = 0
        failed_count = 0

        log.info(f"开始处理 {target_date} 的股票数据，共 {len(stock_list)} 只股票")

        for i in range(0, len(stock_list), batch_size):
            batch_stocks = stock_list[i:i+batch_size]
           
            log.info(f"处理第 {i//batch_size + 1} 批，共 {len(batch_stocks)} 只股票")

            # 尝试批量获取
            batch_df = self.get_hfq_data_batch(batch_stocks, target_date)

            if batch_df is None or batch_df.empty:
                # 批量获取失败，逐只处理
                for stock_code in batch_stocks:
                    try:
                        if self.process_stock(stock_code, target_date):
                            success_count += 1
                        else:
                            failed_count += 1
                    except Exception:
                        log.exception(f"处理 {stock_code} 在 {target_date} 失败")
                        failed_count += 1
            else:
                # 批量获取成功，处理每只股票
                for stock_code in batch_stocks:
                    try:
                        sub_df = batch_df[batch_df['code'] == stock_code]
                        if sub_df.empty:
                            failed_count += 1
                            continue

                        df_qlib = self.format_for_qlib(sub_df, stock_code)
                        if df_qlib is None or df_qlib.empty:
                            failed_count += 1
                            continue

                        csv_path = os.path.join(self.csv_output_dir, f"{stock_code}.csv")
                        if self._save_to_csv(df_qlib, csv_path):
                            success_count += 1
                        else:
                            failed_count += 1
                    except Exception:
                        log.exception(f"处理批量中 {stock_code} 在 {target_date} 失败")
                        failed_count += 1

            # 批次间暂停
            if i + batch_size < len(stock_list):
                time.sleep(0.1)

        log.info(f"处理完成: 成功 {success_count} 只，失败 {failed_count} 只，目标日期: {target_date}")

def main():
   
    cfg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'paths.yaml'))

    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f) or {}

    output_dir = cfg['csv_daily_dir']

    converter = QlibDataConverter(output_dir)
    
    market = 'ALL'
    batch_size = 1000

    # 只处理最新一天的数据
    converter.process_all_stocks(market=market, batch_size=batch_size, target_date=None)
    converter.process_all_indices(market=market, target_date=None)
    
    logging.info("处理完成")
    

if __name__ == "__main__":
    main()