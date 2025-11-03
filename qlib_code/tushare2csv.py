import tushare as ts
import pandas as pd
import os
import time
import logging
from datetime import datetime, timedelta, date
from token_manager import get_valid_token
import tushare as ts
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

class QlibDataConverter:
   
    def __init__(self, token, output_dir):
           
        self.pro = ts.pro_api(token)
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
            
            for stock_code in batch_stocks:
                try:
                    if self.process_stock(stock_code, start_date, end_date):
                        success_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    logging.exception("处理 %s 失败", stock_code)
                    failed_count += 1
                    continue
            
            # 批次间暂停，避免API限制
            if i + batch_size < len(stock_list):
                logging.info("批次间暂停0.2秒...")
                time.sleep(0.2)
        
        logging.info("处理完成: 成功 %d 只，失败 %d 只", success_count, failed_count)
    
     
   

    

def main():
    TUSHARE_TOKEN = get_valid_token()

    output_dir = r"E:\qlib_data\tushare_qlib_data\csv_data"
    converter = QlibDataConverter(TUSHARE_TOKEN, output_dir)
    
    market = 'ALL'
    start_date = '20150101'
    end_date = date.today().strftime('%Y%m%d') 
    batch_size = 200

    converter.process_all_stocks(market=market, start_date=start_date, end_date=end_date, batch_size=batch_size)
    converter.process_all_indices(market=market, start_date=start_date, end_date=end_date)
    logging.info("处理完成")
    

if __name__ == "__main__":
    main()