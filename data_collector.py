"""
数据采集模块 - 支持BaoStock/tushare/FinnHub
"""
import baostock as bs
import tushare as ts
import pandas as pd
import requests
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
from sqlalchemy.orm import Session
from models import Stock, MonthlyKData, Industry
from config import DATA_SOURCE_CONFIG
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataCollector:
    """数据采集器"""
    
    def __init__(self, db: Session):
        self.db = db
        self.config = DATA_SOURCE_CONFIG
        self.primary_source = self.config["primary"]
        self.backup_sources = self.config["backup"]
        self._baostock_logged_in = False
        
    def _init_baostock(self) -> bool:
        """初始化BaoStock"""
        try:
            if not self._baostock_logged_in:
                result = bs.login()
                if result.error_code != '0':
                    logger.error(f"BaoStock登录失败: {result.error_msg}")
                    return False
                self._baostock_logged_in = True
                logger.info("BaoStock登录成功")
            return True
        except Exception as e:
            logger.error(f"BaoStock登录失败: {e}")
            return False
    
    def _logout_baostock(self):
        """登出BaoStock"""
        try:
            if self._baostock_logged_in:
                bs.logout()
                self._baostock_logged_in = False
        except:
            pass
    
    def _init_tushare(self) -> bool:
        """初始化tushare"""
        try:
            token = self.config["tushare"]["api_key"]
            if not token:
                logger.warning("tushare token未配置")
                return False
            ts.set_token(token)
            pro = ts.pro_api()
            logger.info("tushare初始化成功")
            return True
        except Exception as e:
            logger.error(f"tushare初始化失败: {e}")
            return False
    
    def _init_finnhub(self) -> bool:
        """初始化FinnHub"""
        try:
            api_key = self.config["finnhub"]["api_key"]
            if not api_key:
                logger.warning("FinnHub API key未配置")
                return False
            # FinnHub不需要特殊初始化，只需要API key
            logger.info("FinnHub配置成功")
            return True
        except Exception as e:
            logger.error(f"FinnHub初始化失败: {e}")
            return False
    
    def _init_akshare(self) -> bool:
        """初始化AKShare"""
        try:
            import akshare as ak
            logger.info("AKShare初始化成功")
            return True
        except ImportError:
            logger.error("AKShare未安装，请运行: pip install akshare")
            return False
        except Exception as e:
            logger.error(f"AKShare初始化失败: {e}")
            return False
    
    def get_stock_list_baostock(self) -> List[Dict]:
        """从BaoStock获取股票列表"""
        try:
            if not self._init_baostock():
                return []
            
            stock_list = []
            # 获取沪深A股股票列表
            rs = bs.query_all_stock(day=datetime.now().strftime('%Y-%m-%d'))
            
            if rs.error_code != '0':
                logger.error(f"BaoStock查询股票列表失败: {rs.error_msg}")
                return []
            
            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()
                if not row or len(row) < 3:
                    continue
                    
                code = row[0]  # 股票代码，格式可能是 "sh.600000" 或 "sz.000001"
                # BaoStock返回格式: [code, tradeStatus, code_name]
                # row[1] 是交易状态，row[2] 才是股票名称
                name = row[2] if len(row) > 2 and row[2] else ""  # 股票名称
                
                # 处理代码格式
                if code.startswith("sh."):
                    market = "sh"
                    code_clean = code.replace("sh.", "")
                elif code.startswith("sz."):
                    market = "sz"
                    code_clean = code.replace("sz.", "")
                elif code.startswith("6"):
                    market = "sh"
                    code_clean = code
                else:
                    market = "sz"
                    code_clean = code
                
                # 只保留A股（排除指数、基金等）
                if len(code_clean) == 6 and code_clean.isdigit():
                    # 过滤掉指数代码
                    # 399xxx 是深证指数
                    # 名称中包含"指数"的也是指数
                    if code_clean.startswith("399") or "指数" in name:
                        continue
                    
                    # 只保留真正的股票代码范围
                    # 上海：600xxx, 601xxx, 603xxx, 605xxx, 688xxx（科创板）
                    # 深圳：000xxx, 001xxx, 002xxx（中小板）, 300xxx（创业板）
                    is_valid_stock = (
                        code_clean.startswith("600") or
                        code_clean.startswith("601") or
                        code_clean.startswith("603") or
                        code_clean.startswith("605") or
                        code_clean.startswith("688") or  # 科创板
                        (code_clean.startswith("000") and int(code_clean) >= 1000) or  # 深证主板，排除000001-000999（可能是指数）
                        code_clean.startswith("001") or  # 深证主板
                        code_clean.startswith("002") or  # 中小板
                        code_clean.startswith("300")     # 创业板
                    )
                    
                    if not is_valid_stock:
                        continue
                    
                    # 如果名称为空，使用代码作为名称
                    if not name or name.strip() == "":
                        name = code_clean
                    
                    stock_list.append({
                        "code": code_clean,
                        "name": name.strip() if name else code_clean,
                        "market": market
                    })
            
            return stock_list
        except Exception as e:
            logger.error(f"BaoStock获取股票列表失败: {e}", exc_info=True)
            return []
    
    def get_stock_list_tushare(self) -> List[Dict]:
        """从tushare获取股票列表"""
        try:
            if not self._init_tushare():
                return []
            
            pro = ts.pro_api()
            # 获取股票基本信息
            df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,list_date,area,industry')
            
            stock_list = []
            for _, row in df.iterrows():
                code = row['symbol']
                name = row['name']
                market = "sh" if code.startswith("6") else "sz"
                listing_date = datetime.strptime(str(row['list_date']), '%Y%m%d').date() if pd.notna(row['list_date']) else None
                industry = row.get('industry', '')
                
                stock_list.append({
                    "code": code,
                    "name": name,
                    "market": market,
                    "listing_date": listing_date,
                    "industry": industry
                })
            
            return stock_list
        except Exception as e:
            logger.error(f"tushare获取股票列表失败: {e}")
            return []
    
    def get_industry_monthly_k_baostock(self, index_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从BaoStock获取行业指数的前复权月K数据
        
        Args:
            index_code: BaoStock指数代码（如：sh.000006）
            start_date: 开始日期
            end_date: 结束日期
        """
        try:
            if not self._init_baostock():
                return pd.DataFrame()
            
            # 获取前复权月K数据（添加超时保护）
            import threading
            import queue as thread_queue
            
            result_container = {'rs': None, 'error': None, 'completed': False}
            
            def fetch_data():
                try:
                    result_container['rs'] = bs.query_history_k_data_plus(
                        index_code,
                        "date,open,high,low,close,volume,amount,adjustflag",
                        start_date=start_date,
                        end_date=end_date,
                        frequency="m",  # 月K
                        adjustflag="2"  # 前复权
                    )
                    result_container['completed'] = True
                except Exception as e:
                    result_container['error'] = e
                    result_container['completed'] = True
            
            # 在单独线程中执行，设置超时
            fetch_thread = threading.Thread(target=fetch_data, daemon=True)
            fetch_thread.start()
            fetch_thread.join(timeout=6)  # 6秒超时
            
            if not result_container['completed']:
                logger.warning(f"BaoStock获取行业指数 {index_code} 月K数据超时")
                return pd.DataFrame()
            
            if result_container['error']:
                logger.warning(f"BaoStock获取行业指数 {index_code} 月K数据异常: {result_container['error']}")
                return pd.DataFrame()
            
            rs = result_container['rs']
            if not rs or rs.error_code != '0':
                logger.debug(f"BaoStock获取行业指数 {index_code} 月K数据失败: {rs.error_msg if rs else '无响应'}")
                return pd.DataFrame()
            
            data_list = []
            max_iterations = 500  # 防止无限循环
            iteration = 0
            while (rs.error_code == '0') & rs.next() and iteration < max_iterations:
                row = rs.get_row_data()
                if row:
                    data_list.append(row)
                iteration += 1
            
            if not data_list:
                return pd.DataFrame()
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            if df.empty:
                return pd.DataFrame()
                
            df['date'] = pd.to_datetime(df['date'])
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            
            # 转换数据类型
            for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 计算涨跌幅：月K涨跌幅 = (收盘价 - 上月收盘价) / 上月收盘价 × 100%
            df = df.sort_values('date')
            df['prev_close'] = df['close'].shift(1)
            df['pct_change'] = ((df['close'] - df['prev_close']) / df['prev_close'] * 100).round(2)
            df = df.drop('prev_close', axis=1)
            
            return df
        except Exception as e:
            logger.error(f"BaoStock获取行业指数月K数据失败 {index_code}: {e}", exc_info=True)
            return pd.DataFrame()
    
    
    def get_monthly_k_baostock(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从BaoStock获取前复权月K数据"""
        try:
            if not self._init_baostock():
                return pd.DataFrame()
            
            # BaoStock代码格式：sh.600000 或 sz.000001
            market = "sh" if code.startswith("6") else "sz"
            bs_code = f"{market}.{code}"
            
            # 获取前复权月K数据
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume,amount,adjustflag",
                start_date=start_date,
                end_date=end_date,
                frequency="m",  # 月K
                adjustflag="2"  # 前复权
            )
            
            if rs.error_code != '0':
                logger.warning(f"BaoStock获取股票 {code} 月K数据失败: {rs.error_msg}")
                return pd.DataFrame()
            
            data_list = []
            while (rs.error_code == '0') & rs.next():
                row = rs.get_row_data()
                if row:
                    data_list.append(row)
            
            if not data_list:
                return pd.DataFrame()
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            if df.empty:
                return pd.DataFrame()
                
            df['date'] = pd.to_datetime(df['date'])
            df['year'] = df['date'].dt.year
            df['month'] = df['date'].dt.month
            
            # 转换数据类型
            for col in ['open', 'high', 'low', 'close', 'volume', 'amount']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 计算涨跌幅：月K涨跌幅 = (收盘价 - 上月收盘价) / 上月收盘价 × 100%
            df = df.sort_values('date')
            df['prev_close'] = df['close'].shift(1)
            df['pct_change'] = ((df['close'] - df['prev_close']) / df['prev_close'] * 100).round(2)
            df = df.drop('prev_close', axis=1)
            
            return df
        except Exception as e:
            logger.error(f"BaoStock获取月K数据失败 {code}: {e}", exc_info=True)
            return pd.DataFrame()
    
    def get_monthly_k_tushare(self, code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """从tushare获取前复权月K数据"""
        try:
            if not self._init_tushare():
                return pd.DataFrame()
            
            pro = ts.pro_api()
            # tushare代码格式：600000.SH 或 000001.SZ
            market = "SH" if code.startswith("6") else "SZ"
            ts_code = f"{code}.{market}"
            
            # 获取月K数据（需要按月获取）
            start_year = int(start_date[:4])
            start_month = int(start_date[5:7])
            end_year = int(end_date[:4])
            end_month = int(end_date[5:7])
            
            all_data = []
            for year in range(start_year, end_year + 1):
                for month in range(1, 13):
                    if year == start_year and month < start_month:
                        continue
                    if year == end_year and month > end_month:
                        break
                    
                    # tushare获取月线数据
                    try:
                        df = pro.monthly(ts_code=ts_code, start_date=f"{year}{month:02d}01", end_date=f"{year}{month:02d}28")
                        if not df.empty:
                            all_data.append(df)
                        time.sleep(0.2)  # 避免请求过快
                    except:
                        continue
            
            if not all_data:
                return pd.DataFrame()
            
            df = pd.concat(all_data, ignore_index=True)
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            df['year'] = df['trade_date'].dt.year
            df['month'] = df['trade_date'].dt.month
            
            # 重命名列以匹配我们的数据结构
            df = df.rename(columns={
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'vol': 'volume',
                'amount': 'amount',
                'pct_chg': 'pct_change'
            })
            
            return df
        except Exception as e:
            logger.error(f"tushare获取月K数据失败 {code}: {e}")
            return pd.DataFrame()
    
    def get_stock_listing_date(self, code: str) -> Optional[date]:
        """获取股票上市日期"""
        # 优先从BaoStock获取
        try:
            if self._init_baostock():
                market = "sh" if code.startswith("6") else "sz"
                bs_code = f"{market}.{code}"
                rs = bs.query_stock_basic(code=bs_code)
                if rs.error_code == '0':
                    while rs.next():
                        row = rs.get_row_data()
                        if row and len(row) > 1 and row[1]:  # ipoDate
                            listing_date = datetime.strptime(row[1], '%Y-%m-%d').date()
                            return listing_date
        except Exception as e:
            logger.debug(f"获取股票 {code} 上市日期失败: {e}")
            pass
        
        # 备用：从tushare获取
        try:
            if self._init_tushare():
                pro = ts.pro_api()
                market = "SH" if code.startswith("6") else "SZ"
                ts_code = f"{code}.{market}"
                df = pro.stock_basic(ts_code=ts_code, fields='list_date')
                if not df.empty and pd.notna(df.iloc[0]['list_date']):
                    listing_date = datetime.strptime(str(df.iloc[0]['list_date']), '%Y%m%d').date()
                    return listing_date
        except:
            pass
        
        return None
    
    def update_stock_list(self, progress_callback=None) -> int:
        """更新股票列表
        
        Args:
            progress_callback: 进度回调函数，接收(current, total, message)参数
        """
        logger.info("开始更新股票列表...")
        
        if progress_callback:
            progress_callback(0, 100, "正在获取股票列表...")
        
        stock_list = []
        industry_data = {}  # {code: industry_name}
        
        # 优先使用BaoStock获取股票基本信息
        if self.config["baostock"]["enabled"]:
            stock_list = self.get_stock_list_baostock()
            if stock_list:
                logger.info(f"从BaoStock获取到 {len(stock_list)} 只股票")
        
        # 如果akshare可用，优先获取行业信息（免费且稳定）
        if stock_list and self.config["akshare"]["enabled"]:
            try:
                if self._init_akshare():
                    import akshare as ak
                    logger.info("开始从AKShare获取行业信息...")
                    
                    # 方法1: 尝试通过实时行情获取（批量，效率高）
                    try:
                        df = ak.stock_zh_a_spot_em()
                        if df is not None and len(df) > 0:
                            # 检查是否有行业字段
                            industry_col = None
                            for col in ['行业', '所属行业', 'industry']:
                                if col in df.columns:
                                    industry_col = col
                                    break
                            
                            if industry_col:
                                for _, row in df.iterrows():
                                    code = str(row.get('代码', '')).strip()
                                    industry = row.get(industry_col, '')
                                    if code and industry and pd.notna(industry) and str(industry) != 'nan':
                                        industry_data[code] = str(industry).strip()
                                logger.info(f"从AKShare实时行情获取到 {len(industry_data)} 只股票的行业信息")
                    except Exception as e:
                        logger.debug(f"AKShare实时行情方法失败: {e}")
                    
                    # 方法2: 如果方法1失败，尝试逐个获取（较慢但更可靠）
                    if len(industry_data) == 0:
                        logger.info("尝试逐个获取行业信息...")
                        for idx, stock_info in enumerate(stock_list[:500]):  # 限制前500只，避免太慢
                            try:
                                code = stock_info["code"]
                                if len(code) == 6 and code.isdigit():
                                    df = ak.stock_individual_info_em(symbol=code)
                                    if df is not None and len(df) > 0:
                                        industry_row = df[df['item'] == '所属行业']
                                        if len(industry_row) > 0:
                                            industry = industry_row.iloc[0]['value']
                                            if industry and str(industry) != 'nan':
                                                industry_data[code] = str(industry).strip()
                                # 每10只股票休息一下，避免请求过快
                                if (idx + 1) % 10 == 0:
                                    time.sleep(0.3)
                            except Exception as e:
                                logger.debug(f"获取股票 {stock_info['code']} 行业信息失败: {e}")
                                continue
                        logger.info(f"从AKShare逐个获取到 {len(industry_data)} 只股票的行业信息")
            except Exception as e:
                logger.warning(f"从AKShare获取行业信息失败: {e}")
                # 如果是网络/代理错误，给出提示
                if "proxy" in str(e).lower() or "connection" in str(e).lower():
                    logger.warning("AKShare网络连接失败，可能是代理设置问题。建议检查网络环境或稍后重试。")
        
        # 如果tushare可用，尝试获取行业信息补充（作为备用）
        if stock_list and len(industry_data) == 0 and self.config["tushare"]["enabled"]:
            try:
                if self._init_tushare():
                    pro = ts.pro_api()
                    # 获取行业信息
                    df = pro.stock_basic(exchange='', list_status='L', fields='symbol,industry')
                    for _, row in df.iterrows():
                        code = row['symbol']
                        industry = row.get('industry', '')
                        if industry and pd.notna(industry):
                            industry_data[code] = industry
                    logger.info(f"从tushare获取到 {len(industry_data)} 只股票的行业信息")
            except Exception as e:
                logger.warning(f"从tushare获取行业信息失败: {e}")
        
        # 如果BaoStock失败，尝试tushare
        if not stock_list and self.config["tushare"]["enabled"]:
            stock_list = self.get_stock_list_tushare()
            if stock_list:
                logger.info(f"从tushare获取到 {len(stock_list)} 只股票")
        
        if not stock_list:
            logger.error("未能获取股票列表")
            if progress_callback:
                progress_callback(100, 100, "获取股票列表失败")
            return 0
        
        # 补充行业信息到stock_list
        for stock in stock_list:
            code = stock["code"]
            if code in industry_data:
                stock["industry"] = industry_data[code]
            elif "industry" not in stock:
                stock["industry"] = ""
        
        if progress_callback:
            progress_callback(10, 100, f"已获取 {len(stock_list)} 只股票，开始保存到数据库...")
        
        # 保存到数据库
        count = 0
        total = len(stock_list)
        
        # 批量获取已存在的股票代码，避免重复查询
        existing_codes = {s.code for s in self.db.query(Stock.code).all()}
        
        for idx, stock_info in enumerate(stock_list):
            # 更新进度
            if progress_callback and (idx + 1) % 100 == 0:
                progress = 10 + int((idx + 1) / total * 80)
                progress_callback(progress, 100, f"正在处理: {stock_info['code']} - {stock_info['name']} ({idx + 1}/{total})")
            
            code = stock_info["code"]
            
            if code in existing_codes:
                # 更新现有记录
                existing = self.db.query(Stock).filter(Stock.code == code).first()
                if existing:
                    existing.name = stock_info["name"]
                    existing.market = stock_info["market"]
                    if "listing_date" in stock_info and stock_info["listing_date"]:
                        existing.listing_date = stock_info["listing_date"]
                    if "industry" in stock_info and stock_info["industry"]:
                        existing.industry_name = stock_info["industry"]
                    existing.updated_at = datetime.now()
            else:
                # 创建新记录 - 优化：先使用默认日期，避免阻塞
                listing_date = stock_info.get("listing_date")
                if not listing_date:
                    # 对于新股票，先使用默认日期，后续可以批量更新上市日期
                    listing_date = date(2000, 1, 1)  # 默认日期
                
                # 再次检查，避免在批量提交期间重复添加
                check_existing = self.db.query(Stock).filter(Stock.code == code).first()
                if not check_existing:
                    stock = Stock(
                        code=code,
                        name=stock_info["name"],
                        market=stock_info["market"],
                        listing_date=listing_date,
                        industry_name=stock_info.get("industry", "")
                    )
                    self.db.add(stock)
                    count += 1
                    existing_codes.add(code)  # 添加到已存在集合中
                
                # 每100条提交一次，避免内存占用过大
                if count % 100 == 0:
                    try:
                        self.db.commit()
                    except Exception as e:
                        logger.warning(f"批量提交时出错（可能重复）: {e}")
                        self.db.rollback()
                        # 重新获取已存在的代码集合
                        existing_codes = {s.code for s in self.db.query(Stock.code).all()}
        
        self.db.commit()
        
        if progress_callback:
            progress_callback(100, 100, f"更新完成，新增 {count} 只股票")
        
        logger.info(f"更新股票列表完成，新增 {count} 只股票")
        return count
    
    def update_monthly_k_data(self, code: str, force_update: bool = False, progress_callback=None) -> int:
        """更新单只股票的月K数据"""
        try:
            stock = self.db.query(Stock).filter(Stock.code == code).first()
            if not stock:
                logger.warning(f"股票 {code} 不存在")
                return 0
            
            # 确定更新日期范围
            if force_update:
                start_date = stock.listing_date.strftime('%Y-%m-%d')
            else:
                # 获取已有数据的最新日期
                latest = self.db.query(MonthlyKData).filter(
                    MonthlyKData.stock_code == code
                ).order_by(MonthlyKData.year.desc(), MonthlyKData.month.desc()).first()
                
                if latest:
                    # 从下一个月开始更新
                    if latest.month == 12:
                        start_year = latest.year + 1
                        start_month = 1
                    else:
                        start_year = latest.year
                        start_month = latest.month + 1
                    start_date = f"{start_year}-{start_month:02d}-01"
                else:
                    start_date = stock.listing_date.strftime('%Y-%m-%d')
            
            end_date = datetime.now().strftime('%Y-%m-%d')
            
            # 检查是否需要更新（如果开始日期晚于结束日期，说明已经是最新数据）
            if start_date > end_date:
                logger.debug(f"股票 {code} 数据已是最新，无需更新")
                return 0
            
            # 获取数据
            df = pd.DataFrame()
            
            if self.config["baostock"]["enabled"]:
                df = self.get_monthly_k_baostock(code, start_date, end_date)
            
            if df.empty and self.config["tushare"]["enabled"]:
                df = self.get_monthly_k_tushare(code, start_date, end_date)
            
            if df.empty:
                logger.warning(f"未能获取股票 {code} 的月K数据（{start_date} 至 {end_date}）")
                if progress_callback:
                    progress_callback(100, 100, f"未能获取股票 {code} 的月K数据")
                return 0
            
            if progress_callback:
                progress_callback(50, 100, f"已获取 {len(df)} 条数据，正在保存...")
            
            # 保存到数据库
            count = 0
            total_rows = len(df)
            for idx, (_, row) in enumerate(df.iterrows()):
                try:
                    existing = self.db.query(MonthlyKData).filter(
                        MonthlyKData.stock_code == code,
                        MonthlyKData.year == int(row['year']),
                        MonthlyKData.month == int(row['month'])
                    ).first()
                    
                    if existing:
                        # 更新现有记录
                        existing.open_price = float(row.get('open', 0)) if pd.notna(row.get('open')) else None
                        existing.close_price = float(row.get('close', 0)) if pd.notna(row.get('close')) else None
                        existing.high_price = float(row.get('high', 0)) if pd.notna(row.get('high')) else None
                        existing.low_price = float(row.get('low', 0)) if pd.notna(row.get('low')) else None
                        existing.volume = float(row.get('volume', 0)) if pd.notna(row.get('volume')) else None
                        existing.amount = float(row.get('amount', 0)) if pd.notna(row.get('amount')) else None
                        existing.pct_change = float(row.get('pct_change', 0)) if pd.notna(row.get('pct_change')) else None
                        existing.updated_at = datetime.now()
                    else:
                        # 创建新记录
                        monthly_data = MonthlyKData(
                            stock_code=code,
                            year=int(row['year']),
                            month=int(row['month']),
                            open_price=float(row.get('open', 0)) if pd.notna(row.get('open')) else None,
                            close_price=float(row.get('close', 0)) if pd.notna(row.get('close')) else None,
                            high_price=float(row.get('high', 0)) if pd.notna(row.get('high')) else None,
                            low_price=float(row.get('low', 0)) if pd.notna(row.get('low')) else None,
                            volume=float(row.get('volume', 0)) if pd.notna(row.get('volume')) else None,
                            amount=float(row.get('amount', 0)) if pd.notna(row.get('amount')) else None,
                            pct_change=float(row.get('pct_change', 0)) if pd.notna(row.get('pct_change')) else None
                        )
                        self.db.add(monthly_data)
                        count += 1
                        
                        # 每50条提交一次，避免内存占用过大
                        if count % 50 == 0:
                            self.db.commit()
                            if progress_callback:
                                progress = 50 + int((idx + 1) / total_rows * 50)
                                progress_callback(progress, 100, f"已保存 {idx + 1}/{total_rows} 条数据...")
                except Exception as e:
                    logger.error(f"保存股票 {code} {row.get('year')}-{row.get('month')} 数据失败: {e}")
                    continue
            
            self.db.commit()
            
            if progress_callback:
                progress_callback(100, 100, f"更新完成，新增 {count} 条记录")
            
            if count > 0:
                logger.info(f"更新股票 {code} 月K数据完成，新增 {count} 条记录")
            return count
        except Exception as e:
            logger.error(f"更新股票 {code} 月K数据时发生错误: {e}", exc_info=True)
            self.db.rollback()
            return 0

