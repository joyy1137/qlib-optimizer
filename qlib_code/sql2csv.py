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
       
        self.csv_output_dir = output_dir
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

        self.csv_output_dir = output_dir
        os.makedirs(self.csv_output_dir, exist_ok=True)
        
        
    def get_hfq_data_from_sql(self, code, start_date, end_date):
        """
         从SQL数据库获取复权后数据
        """
        if self.engine is None:
            log.error("数据库未连接")
            return None
            
        try:
            # 转换日期格式
            start_date_str = datetime.strptime(start_date, '%Y%m%d').strftime('%Y-%m-%d')
            end_date_str = datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
            
            # 查询股票数据
            query = text("""
                SELECT valuation_date, code, open, high, low, close, volume, amt, adjfactor_jy 
                FROM data_stock 
                WHERE code = :code 
                AND valuation_date BETWEEN :start_date AND :end_date
                ORDER BY valuation_date
            """)
            
            df = pd.read_sql(query, self.engine, 
                           params={'code': code, 'start_date': start_date_str, 'end_date': end_date_str})
            
            if df is not None and not df.empty:
                log.info(f"从数据库获取 {code} 数据成功，共 {len(df)} 条记录")
                return df
            # else:
            #     log.info(f"股票 {code} 在数据库中无数据")
            #     return None
                
        except Exception as e:
            log.error(f"从数据库获取 {code} 数据失败: {e}")
            return None

    def get_hfq_data_batch(self, codes, start_date, end_date):
  
        if not codes:
            return None
        try:
            # 转换日期格式
            start_date_str = datetime.strptime(start_date, '%Y%m%d').strftime('%Y-%m-%d')
            end_date_str = datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
            
            # 构建IN查询条件
            codes_str = ','.join([f"'{code}'" for code in codes])
            
            query = text(f"""
                SELECT valuation_date, code, open, high, low, close, volume, amt, adjfactor_jy 
                FROM data_stock 
                WHERE code IN ({codes_str})
                AND valuation_date BETWEEN :start_date AND :end_date
                ORDER BY code, valuation_date
            """)
            
            df = pd.read_sql(query, self.engine, 
                           params={'start_date': start_date_str, 'end_date': end_date_str})
            
            if df is not None and not df.empty:
                log.info(f"批量获取 {len(codes)} 只股票数据成功，共 {len(df)} 条记录")
                return df
            else:
                log.warning("批量查询返回空数据")
                return None
                
        except Exception as e:
            log.error(f"批量获取数据失败: {e}，回退到逐只获取")
            return self._fallback_to_individual_queries(codes, start_date, end_date)
        
    
    def _fallback_to_individual_queries(self, codes, start_date, end_date):
        """
        回退到逐只股票查询
        """
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(codes))) as executor:
            future_to_code = {executor.submit(self.get_hfq_data_from_sql, code, start_date, end_date): code 
                            for code in codes}
            
            for future in concurrent.futures.as_completed(future_to_code):
                code = future_to_code[future]
                try:
                    result = future.result()
                    if result is not None and not result.empty:
                        results.append(result)
                except Exception as e:
                    log.error(f"查询 {code} 失败: {e}")
        
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
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        
        required_columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'factor']
        if 'money' in df.columns:
            required_columns.append('money')
        
        df = df[required_columns]
        return df
    
    def process_stock(self, code, start_date=None, end_date=None):
        """
        处理单只股票：只获取最近数据，如果已存在相同日期则覆盖
        """
        csv_path = os.path.join(self.csv_output_dir, f"{code}.csv")


        today = date.today()
        two_days_ago = today - timedelta(days=7)

        start_date = two_days_ago.strftime('%Y%m%d')
        end_date = today.strftime('%Y%m%d')

        # 从数据库获取数据
        df = self.get_hfq_data_from_sql(code, start_date, end_date)
        if df is None or df.empty:
            return False

        df_qlib = self.format_for_qlib(df, code)
        if df_qlib is None:
            return False

        return self._save_to_csv(df_qlib, csv_path)


    # def process_stock(self, code, start_date='20200101', end_date='20250920'):
    #     """
    #     处理单只股票
    #     """
    #     csv_path = os.path.join(self.csv_output_dir, f"{code}.csv")
        
    #     # 检查现有数据，实现增量更新
    #     if os.path.exists(csv_path):
    #         try:
    #             existing = pd.read_csv(csv_path, parse_dates=['date'])
    #             if not existing.empty:
    #                 last_date = existing['date'].max().date()
    #                 end_dt = datetime.strptime(end_date, '%Y%m%d').date()
    #                 if last_date >= end_dt:
    #                     log.info(f"{code} 数据已是最新，跳过")
    #                     return True
                   
    #                 new_start = last_date + timedelta(days=1)
    #                 start_date = new_start.strftime('%Y%m%d')
    #                 log.info(f"{code} 存在历史数据，从 {start_date} 开始提取")
    #         except Exception as e:
    #             log.warning(f"读取现有CSV失败，执行全量拉取: {e}")

    #     # 从数据库获取数据
    #     df = self.get_hfq_data_from_sql(code, start_date, end_date)
    #     if df is None or df.empty:
    #         return False
        
    #     df_qlib = self.format_for_qlib(df, code)
    #     if df_qlib is None:
    #         return False
            
    #     # 保存数据
    #     return self._save_to_csv(df_qlib, csv_path)
     


    def _save_to_csv(self, df, csv_path):
        """
        保存数据到CSV文件
        """
        try:
            if os.path.exists(csv_path):
                existing = pd.read_csv(csv_path, parse_dates=['date'])
                combined = pd.concat([existing, df], ignore_index=True)
                combined['date'] = pd.to_datetime(combined['date']).dt.strftime('%Y-%m-%d')
                combined.drop_duplicates(subset=['date'], keep='last', inplace=True)
                combined.sort_values('date', inplace=True)
                combined.to_csv(csv_path, index=False)
            else:
                df.sort_values('date', inplace=True)
                df.to_csv(csv_path, index=False)
                
            # log.info(f"成功保存数据到 {csv_path}")
            return True
        except Exception as e:
            log.error(f"写入CSV失败 ({csv_path}): {e}")
            return False

    def get_index_data(self, index_code, start_date='20200101', end_date='20250920'):
        """
        从数据库获取指数数据
        """
        if self.engine is None:
            return None
            
        try:
            # 转换日期格式
            start_date_str = datetime.strptime(start_date, '%Y%m%d').strftime('%Y-%m-%d')
            end_date_str = datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
            
            # 查询指数数据 - 根据你的表结构调整字段名
            query = text("""
                SELECT valuation_date, code, open, high, low, close, volume, amt 
                FROM data_index 
                WHERE code = :index_code 
                AND valuation_date BETWEEN :start_date AND :end_date
                ORDER BY valuation_date
            """)
            
            df = pd.read_sql(query, self.engine, 
                           params={'index_code': index_code, 'start_date': start_date_str, 'end_date': end_date_str})
            
            # if df is None or df.empty:
            #     log.warning(f"指数 {index_code} 无数据")
            #     return None

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
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

            required_columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'factor']
            if 'money' in df.columns:
                required_columns.append('money')

            df = df[required_columns]
            return df
        except Exception as e:
            log.exception(f"获取指数 {index_code} 数据失败: {e}")
            return None

    def process_index(self, index_code, start_date='20200101', end_date='20250920'):
        """处理单个指数数据"""
        csv_path = os.path.join(self.csv_output_dir, f"{index_code}.csv")
        df = self.get_index_data(index_code, start_date, end_date)
        if df is None or df.empty:
            return False

        return self._save_to_csv(df, csv_path)

    def process_all_indices(self, market='ALL', start_date='20200101', end_date='20250920'):
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

        for idx in index_codes:
            try:
                if self.process_index(idx, start_date=start_date, end_date=end_date):
                    log.info(f"处理指数 {idx} 成功")
                else:
                    log.error(f"处理指数 {idx} 失败")
            except Exception:
                log.exception(f"处理指数 {idx} 失败")
    
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
                # 获取指数成分股 - 这里需要根据你的成分股表结构调整
                # index_map = {'zz500': '000905.SH', 'hs300': '000300.SH', 'sz50': '000016.SH','zz1000':'000852.SH','zz2000':'932000.CSI'}
                # index_code = index_map[market]
                
                query = text("""
                    SELECT DISTINCT code 
                    FROM data.indexcomponent 
                    WHERE organization = :market
                    ORDER BY code
                """)
                df = pd.read_sql(query, self.engine)
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

    def process_all_stocks(self, market='ALL', start_date='20200101', end_date='20250920', batch_size=10):
        """
        处理全部股票
        """
        stock_list = self.get_all_stocks(market)
        if not stock_list:
            log.error("未获取到股票列表")
            return

        success_count = 0
        failed_count = 0

        for i in range(0, len(stock_list), batch_size):
            batch_stocks = stock_list[i:i+batch_size]
           
            log.info(f"处理第 {i//batch_size + 1} 批，共 {len(batch_stocks)} 只股票")

            # 尝试批量获取
            batch_df = self.get_hfq_data_batch(batch_stocks, start_date, end_date)

            if batch_df is None or batch_df.empty:
                # 批量获取失败，逐只处理
                for stock_code in batch_stocks:
                    try:
                        if self.process_stock(stock_code, start_date, end_date):
                            success_count += 1
                        else:
                            failed_count += 1
                    except Exception:
                        log.exception(f"处理 {stock_code} 失败")
                        failed_count += 1
            else:
                # 批量获取成功，处理每只股票
                for stock_code in batch_stocks:
                    try:
                        sub_df = batch_df[batch_df['code'] == stock_code]
                        if sub_df.empty:
                            # log.info(f"批量数据中 {stock_code} 无数据")
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
                        log.exception(f"处理批量中 {stock_code} 失败")
                        failed_count += 1

            # 批次间暂停
            if i + batch_size < len(stock_list):
                time.sleep(0.1)

        log.info(f"处理完成: 成功 {success_count} 只，失败 {failed_count} 只")

   

    

def main():
   
    cfg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'paths.yaml'))

    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f) or {}

    output_dir = cfg['csv_output_dir']

    converter = QlibDataConverter(output_dir)
    
    market = 'ALL'
    start_date = '20150101'
    end_date = date.today().strftime('%Y%m%d') 
    batch_size = 1000

    # converter.process_all_stocks(market=market, start_date=start_date, end_date=end_date, batch_size=batch_size)
    converter.process_all_stocks(market=market, batch_size=batch_size)

    converter.process_all_indices(market=market, start_date=start_date, end_date=end_date)
    logging.info("处理完成")
    

if __name__ == "__main__":
    main()