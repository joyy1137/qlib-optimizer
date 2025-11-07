import tinyshare as ts
import pandas as pd
import os
import time
import logging
import concurrent.futures
from datetime import datetime, timedelta, date
import tinyshare as ts
import yaml
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

class QlibDataConverter:
   
    def __init__(self, output_dir):
           
 
        # try:
        self.pro = ts.pro_api("YvyuR14HT1dc75jp0DB7a1xf1di5L4s3ev3n5b0y0KOO4msoed2oUbvD4d852ee6")
        # except Exception:
        #
        #     self.pro = None

        self.csv_output_dir = output_dir

        os.makedirs(self.csv_output_dir, exist_ok=True)
        
        
    
    def get_hfq_data_directly(self, ts_code, start_date, end_date):
        """
        获取Tushare的后复权数据和复权因子
        """
        try:
            
            # 获取后复权数据
            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                adj='hfq'  
            )
            
            if df is not None and not df.empty:
               
                
                # 获取复权因子数据
                try:
                 
                    adj_df = self.pro.adj_factor(
                        ts_code=ts_code,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    if adj_df is not None and not adj_df.empty:
                        # 合并复权因子数据
                        df = df.merge(adj_df[['trade_date', 'adj_factor']], on='trade_date', how='left')
                       
                        return df
                    else:
                        logging.warning("无法获取 %s 复权因子数据，跳过该股票", ts_code)
                        return None
                        
                except Exception as e:
                    logging.warning("获取 %s 复权因子失败，跳过该股票: {e}", ts_code)
                    return None
            else:
                logging.info("股票 %s 无后复权数据", ts_code)
                return None
        except Exception as e:
            logging.warning("获取 %s 后复权数据失败: {e}", ts_code)
            return None

    def get_hfq_data_batch(self, ts_codes, start_date, end_date):
        """
        尝试批量拉取多个 ts_code 的后复权数据和复权因子。
        优先使用一次 API 批量拉取（将 ts_code 拼接为逗号分隔字符串），如果 API 不支持则回退到并发逐只拉取。
        返回合并后的 DataFrame，包含列 'ts_code','trade_date',...,'adj_factor'。
        """
        if not ts_codes:
            return None

        if self.pro is None:
            logging.error("pro API 未初始化，无法拉取数据")
            return None

        try:
            # 先尝试使用一次性批量请求
            ts_param = ','.join(ts_codes)
            df = self.pro.daily(ts_code=ts_param, start_date=start_date, end_date=end_date, adj='hfq')
           
            if df is None or df.empty:
                raise ValueError("批量 daily 返回空")

            try:
                adj_df = self.pro.adj_factor(ts_code=ts_param, start_date=start_date, end_date=end_date)
                if adj_df is not None and not adj_df.empty:
                    df = df.merge(adj_df[['ts_code', 'trade_date', 'adj_factor']], on=['ts_code', 'trade_date'], how='left')
                else:
                    # 如果没有复权因子，补充默认 1.0
                    logging.warning("批量获取 adj_factor 失败")
         
            except Exception:
                logging.warning("批量获取 adj_factor 失败，继续使用 daily 数据并设置 factor=1.0")
                

            return df
        except Exception as e:
            logging.info("批量接口失败或不支持批量（%s），回退到并发逐只请求", e)

    
        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(ts_codes))) as executor:
            futs = {executor.submit(self.get_hfq_data_directly, code, start_date, end_date): code for code in ts_codes}
            for fut in concurrent.futures.as_completed(futs):
                code = futs[fut]
                try:
                    res = fut.result()
                    if res is not None and not res.empty:
                        # 在单只结果中添加 ts_code 列（如果没有）
                        if 'ts_code' not in res.columns:
                            res['ts_code'] = code
                        results.append(res)
                except Exception:
                    logging.exception("回退并发请求 %s 失败", code)

        if not results:
            return None

        all_df = pd.concat(results, ignore_index=True)
        return all_df
    
    
    def format_for_qlib(self, df, ts_code):
        """
        格式化为Qlib需要的格式
        """
        # 检查df是否为None或空
        if df is None or df.empty:
            return None
       
        column_mapping = {
            'trade_date': 'date',
            'open': 'open',      
            'close': 'close', 
            'high': 'high',
            'low': 'low',
            'vol': 'volume',
            'amount': 'money',
            'adj_factor': 'factor',
        }
     
        
        df = df.rename(columns=column_mapping)
        
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
        
        required_columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'factor']
        if 'money' in df.columns:
            required_columns.append('money')
        
        df = df[required_columns]
        
    
        
        return df

    def process_stock(self, ts_code, start_date='20200101', end_date='20250920'):

        csv_path = os.path.join(self.csv_output_dir, f"{ts_code}.csv")
        if os.path.exists(csv_path):
            try:
                existing = pd.read_csv(csv_path, parse_dates=['date'])
                if not existing.empty:
                    last_date = existing['date'].max().date()
                    end_dt = datetime.strptime(end_date, '%Y%m%d').date()
                    if last_date >= end_dt:
                        
                        return True
                   
                    new_start = last_date + timedelta(days=1)
                    start_date = new_start.strftime('%Y%m%d')
                    logging.info("%s 存在历史数据，从 %s 开始提取", ts_code, start_date)
            except Exception as e:
                logging.warning("读取现有CSV失败，执行全量拉取: %s", e)

        df = self.get_hfq_data_directly(ts_code, start_date, end_date)
        if df is None or df.empty:
            return False
        
        df_qlib = self.format_for_qlib(df, ts_code)
        if df_qlib is None:
            return False
        if os.path.exists(csv_path):
            try:
                existing = pd.read_csv(csv_path, parse_dates=['date'])
                combined = pd.concat([existing, df_qlib], ignore_index=True)
                combined['date'] = pd.to_datetime(combined['date']).dt.strftime('%Y-%m-%d')
                combined.drop_duplicates(subset=['date'], keep='last', inplace=True)
                combined.sort_values('date', inplace=True)
                combined.to_csv(csv_path, index=False)
                
            except Exception as e:
                logging.error("写入CSV失败 (%s): %s", csv_path, e)
                return False
        else:
            try:
                df_qlib.sort_values('date', inplace=True)
                df_qlib.to_csv(csv_path, index=False)
               
            except Exception as e:
                logging.error("写入CSV失败 (%s): %s", csv_path, e)
                return False
        
       
        
        return True
    
    def convert_ts_code(self, ts_code):
        code, exchange = ts_code.split('.')
        return f'{exchange}{code}'

    def get_index_data(self, index_code, start_date='20200101', end_date='20250920'):
     
        try:
            df = self.pro.index_daily(ts_code=index_code, start_date=start_date, end_date=end_date)
            if df is None or df.empty:
                log.warning("指数 %s 无数据", index_code)
                return None

            df['adj_factor'] = 1.0

            column_mapping = {
                'trade_date': 'date',
                'open': 'open',      
                'close': 'close', 
                'high': 'high',
                'low': 'low',
                'vol': 'volume',
                'amount': 'money',
                'adj_factor': 'factor',
            }
            df = df.rename(columns=column_mapping)
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

            required_columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'factor']
            if 'money' in df.columns:
                required_columns.append('money')

            df = df[required_columns]
            return df
        except Exception as e:
            log.exception("获取指数 %s 数据失败: %s", index_code, e)
            return None

    def process_index(self, index_code, start_date='20200101', end_date='20250920'):
        """将单个指数的数据保存为 csv（Qlib 格式）"""
        csv_path = os.path.join(self.csv_output_dir, f"{index_code}.csv")
        df = self.get_index_data(index_code, start_date, end_date)
        if df is None or df.empty:
            return False

        try:
            df.sort_values('date', inplace=True)
            df.to_csv(csv_path, index=False)
            log.info("已保存指数 %s 到 %s", index_code, csv_path)
            return True
        except Exception as e:
            log.exception("写入指数CSV失败 (%s): %s", csv_path, e)
            return False

    def process_all_indices(self, market='ALL', start_date='20200101', end_date='20250920'):
       
        mapping = {
            'zz500': ['000905.SH'],
            'hs300': ['000300.SH'],
            'sz50': ['000016.SH'],
            'ALL': ['000905.SH', '000300.SH', '000016.SH'],
        }

        if isinstance(market, (list, tuple)):
            index_codes = market
        else:
            index_codes = mapping.get(market, [market])

        for idx in index_codes:
            try:
                self.process_index(idx, start_date=start_date, end_date=end_date)
            except Exception:
                log.exception("处理指数 %s 失败", idx)
    
    def get_all_stocks(self, market='ALL'):
        """
        获取股票列表
        
        """
        log.info(f"正在获取 {market} 股票列表...")
        
        if market == 'zz500':
            # 获取中证500成分股
            df = self.pro.index_weight(index_code='000905.SH')
            stock_list = df['con_code'].unique().tolist()
        elif market == 'hs300':
            # 获取沪深300成分股
            df = self.pro.index_weight(index_code='000300.SH')
            stock_list = df['con_code'].unique().tolist()
        elif market == 'sz50':
            # 获取上证50成分股
            df = self.pro.index_weight(index_code='000016.SH')
            stock_list = df['con_code'].unique().tolist()
        else:
            # 获取所有A股
            df = self.pro.stock_basic(exchange='', list_status='L')
            stock_list = df['ts_code'].tolist()
        
        log.info(f"获取到 {len(stock_list)} 只股票")
        return stock_list
    
    def process_all_stocks(self, market='ALL', start_date='20200101', end_date='20250920', batch_size=50):
        """
        处理全部股票
        
        """
        # 获取股票列表
        stock_list = self.get_all_stocks(market)

        success_count = 0
        failed_count = 0

        for i in range(0, len(stock_list), batch_size):
            batch_stocks = stock_list[i:i+batch_size]

            log.info(f"处理第 {i//batch_size + 1} 批，共 {len(batch_stocks)} 只股票")

            # 尝试使用批量接口一次性拉取本批次所有股票的数据
            batch_df = None
            try:
                batch_df = self.get_hfq_data_batch(batch_stocks, start_date, end_date)
            except Exception:
                logging.exception("批量拉取失败，回退到逐只处理")

            if batch_df is None or batch_df.empty:
                # 如果批量接口不可用或返回空，退回到逐只处理（保持兼容）
                for stock_code in batch_stocks:
                    try:
                        if self.process_stock(stock_code, start_date, end_date):
                            success_count += 1
                        else:
                            failed_count += 1
                    except Exception:
                        logging.exception("处理 %s 失败", stock_code)
                        failed_count += 1
                        continue
            else:
                # 批量结果中可能包含多支股票，按 ts_code 切分并写入对应 CSV
                # 确保列名包含 ts_code 与 trade_date
                if 'ts_code' not in batch_df.columns:
                    logging.error("批量返回数据缺少 ts_code 列，回退到逐只处理")
                    for stock_code in batch_stocks:
                        try:
                            if self.process_stock(stock_code, start_date, end_date):
                                success_count += 1
                            else:
                                failed_count += 1
                        except Exception:
                            logging.exception("处理 %s 失败", stock_code)
                            failed_count += 1
                            continue
                else:
                    # 统一处理每只股票在 batch_df 中的分片
                    for stock_code in batch_stocks:
                        try:
                            sub_df = batch_df[batch_df['ts_code'] == stock_code]
                            if sub_df is None or sub_df.empty:
                                logging.info("批量数据中 %s 无数据，跳过", stock_code)
                                failed_count += 1
                                continue

                            # 子 DataFrame 里 trade_date 已存在，重命名/格式化并写入 CSV（复用 format_for_qlib）
                            df_qlib = self.format_for_qlib(sub_df, stock_code)
                            if df_qlib is None or df_qlib.empty:
                                failed_count += 1
                                continue

                            csv_path = os.path.join(self.csv_output_dir, f"{stock_code}.csv")
                            if os.path.exists(csv_path):
                                try:
                                    existing = pd.read_csv(csv_path, parse_dates=['date'])
                                    combined = pd.concat([existing, df_qlib], ignore_index=True)
                                    combined['date'] = pd.to_datetime(combined['date']).dt.strftime('%Y-%m-%d')
                                    combined.drop_duplicates(subset=['date'], keep='last', inplace=True)
                                    combined.sort_values('date', inplace=True)
                                    combined.to_csv(csv_path, index=False)
                                    success_count += 1
                                except Exception as e:
                                    logging.error("写入CSV失败 (%s): %s", csv_path, e)
                                    failed_count += 1
                            else:
                                try:
                                    df_qlib.sort_values('date', inplace=True)
                                    df_qlib.to_csv(csv_path, index=False)
                                    success_count += 1
                                except Exception as e:
                                    logging.error("写入CSV失败 (%s): %s", csv_path, e)
                                    failed_count += 1
                        except Exception:
                            logging.exception("处理批量中 %s 失败", stock_code)
                            failed_count += 1

            # 批次间暂停，避免API限制
            if i + batch_size < len(stock_list):
                logging.info("批次间暂停0.2秒...")
                time.sleep(0.2)

        logging.info("处理完成: 成功 %d 只，失败 %d 只", success_count, failed_count)
    
     
   

    

def main():
   
    cfg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'paths.yaml'))

    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f) or {}

    output_dir = cfg['csv_output_dir']

    converter = QlibDataConverter(output_dir)
    
    market = 'ALL'
    start_date = '20150101'
    end_date = date.today().strftime('%Y%m%d') 
    batch_size = 200

    converter.process_all_stocks(market=market, start_date=start_date, end_date=end_date, batch_size=batch_size)
    converter.process_all_indices(market=market, start_date=start_date, end_date=end_date)
    logging.info("处理完成")
    

if __name__ == "__main__":
    main()