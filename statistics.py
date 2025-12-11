"""
统计分析模块
"""
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
from models import Stock, MonthlyKData, Industry
from datetime import date, datetime
from typing import List, Dict, Optional, Tuple
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StatisticsCalculator:
    """统计分析计算器"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def _calculate_statistics_from_data(self, monthly_data: List) -> Optional[Dict]:
        """从月K数据计算统计信息（内部方法）"""
        if not monthly_data:
            return None
        
        up_count = 0
        down_count = 0
        up_pct_sum = 0.0
        down_pct_sum = 0.0
        years = set()
        
        for data in monthly_data:
            years.add(data.year)
            if data.pct_change is not None:
                if data.pct_change > 0:
                    up_count += 1
                    up_pct_sum += data.pct_change
                elif data.pct_change < 0:
                    down_count += 1
                    down_pct_sum += data.pct_change
        
        total_count = up_count + down_count
        if total_count == 0:
            return None
        
        up_probability = (up_count / total_count * 100) if total_count > 0 else 0
        down_probability = (down_count / total_count * 100) if total_count > 0 else 0
        avg_up_pct = (up_pct_sum / up_count) if up_count > 0 else 0
        avg_down_pct = (down_pct_sum / down_count) if down_count > 0 else 0
        
        year_range = f"{min(years)}-{max(years)}" if years else ""
        
        return {
            "up_count": up_count,
            "down_count": down_count,
            "total_count": total_count,
            "up_probability": round(up_probability, 2),
            "down_probability": round(down_probability, 2),
            "avg_up_pct": round(avg_up_pct, 2),
            "avg_down_pct": round(avg_down_pct, 2),
            "year_range": year_range,
            "years_count": len(years)
        }
    
    def calculate_stock_statistics(
        self,
        stock_code: str,
        months: Optional[List[int]] = None,
        min_total_count: int = 0,
        group_by_month: bool = False
    ) -> Optional[Dict]:
        """计算单只股票的统计信息
        
        Args:
            stock_code: 股票代码
            months: 月份列表，None表示所有月份
            min_total_count: 最小总涨跌次数
            group_by_month: True=按月统计，False=汇总统计
        """
        stock = self.db.query(Stock).filter(Stock.code == stock_code).first()
        if not stock:
            return None
        
        # 构建查询条件
        query = self.db.query(MonthlyKData).filter(
            MonthlyKData.stock_code == stock_code
        )
        
        if months:
            query = query.filter(MonthlyKData.month.in_(months))
        
        # 获取所有月K数据
        monthly_data = query.order_by(MonthlyKData.year, MonthlyKData.month).all()
        
        if not monthly_data:
            return None
        
        base_info = {
            "stock_code": stock_code,
            "stock_name": stock.name,
            "market": stock.market,
            "listing_date": stock.listing_date.strftime('%Y-%m-%d'),
        }
        
        if group_by_month and months and len(months) > 1:
            # 按月统计模式
            monthly_stats = {}
            for month in months:
                month_data = [d for d in monthly_data if d.month == month]
                stats = self._calculate_statistics_from_data(month_data)
                if stats:
                    monthly_stats[month] = stats
            
            if not monthly_stats:
                return None
            
            # 检查最小涨跌次数（检查汇总数据）
            all_stats = self._calculate_statistics_from_data(monthly_data)
            if all_stats and min_total_count > 0 and all_stats["total_count"] < min_total_count:
                return None
            
            return {
                **base_info,
                "statistics_mode": "monthly",
                "monthly_statistics": monthly_stats,
                "summary_statistics": all_stats  # 同时提供汇总统计作为参考
            }
        else:
            # 汇总统计模式
            stats = self._calculate_statistics_from_data(monthly_data)
            if not stats:
                return None
            
            # 检查最小涨跌次数
            if min_total_count > 0 and stats["total_count"] < min_total_count:
                return None
            
            return {
                **base_info,
                "statistics_mode": "summary",
                **stats
            }
    
    def calculate_batch_statistics(
        self,
        months: Optional[List[int]] = None,
        market: Optional[str] = None,
        industry_code: Optional[str] = None,
        min_total_count: int = 0,
        exclude_delisted: bool = True,
        limit: int = 20,
        order_by: str = "up_probability"
    ) -> List[Dict]:
        """批量计算股票统计信息"""
        # 构建股票查询
        stock_query = self.db.query(Stock)
        
        if market:
            stock_query = stock_query.filter(Stock.market == market)
        
        if industry_code:
            stock_query = stock_query.filter(Stock.industry_code == industry_code)
        
        if exclude_delisted:
            stock_query = stock_query.filter(Stock.is_delisted == 0)
        
        stocks = stock_query.all()
        
        results = []
        for stock in stocks:
            stats = self.calculate_stock_statistics(
                stock.code,
                months=months,
                min_total_count=min_total_count
            )
            
            if stats:
                results.append(stats)
        
        # 排序
        reverse = True if order_by in ["up_probability", "avg_up_pct"] else False
        results.sort(key=lambda x: x.get(order_by, 0), reverse=reverse)
        
        # 添加排名
        for i, result in enumerate(results[:limit], 1):
            result["rank"] = i
        
        return results[:limit]
    
    def calculate_industry_statistics(
        self,
        industry_code: str,
        months: Optional[List[int]] = None,
        min_total_count: int = 0,
        group_by_month: bool = False
    ) -> Optional[Dict]:
        """计算行业统计信息 - 基于行业板块的月K数据
        
        统计行业板块在各个月份的涨跌情况：
        - 上涨次数：该月份中，板块涨跌幅>0的次数
        - 下跌次数：该月份中，板块涨跌幅<0的次数
        - 上涨概率：上涨次数 / 总次数 × 100%
        - 下跌概率：下跌次数 / 总次数 × 100%
        - 平均涨幅：所有上涨月份的平均涨跌幅
        - 平均跌幅：所有下跌月份的平均涨跌幅
        
        Args:
            industry_code: 行业代码
            months: 月份列表，None表示所有月份
            min_total_count: 最小总涨跌次数
            group_by_month: True=按月统计，False=汇总统计
        """
        from data_collector import DataCollector
        from industry_index_mapping import get_index_code
        from datetime import datetime
        
        industry = self.db.query(Industry).filter(Industry.code == industry_code).first()
        if not industry:
            return None
        
        industry_name = industry.name
        
        # 获取行业对应的BaoStock指数代码
        index_code = get_index_code(industry_name)
        if not index_code:
            logger.warning(f"未找到行业 '{industry_name}' 对应的指数代码")
            return None
        
        logger.info(f"行业板块 '{industry_name}' 使用BaoStock指数代码: {index_code}")
        
        # 获取行业板块的月K数据
        collector = DataCollector(self.db)
        start_date = "2000-01-01"
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        # 从BaoStock获取行业板块月K数据
        df = collector.get_industry_monthly_k_baostock(index_code, start_date, end_date)
        
        if df is None or len(df) == 0:
            logger.warning(f"未能获取行业板块 {index_code} ({industry_name}) 的月K数据")
            return None
        
        # 转换为月度数据列表
        monthly_data = []
        for _, row in df.iterrows():
            if pd.notna(row.get('pct_change')):
                monthly_data.append({
                    'year': int(row['year']),
                    'month': int(row['month']),
                    'pct_change': float(row['pct_change'])
                })
        
        if not monthly_data:
            return None
        
        # 按月份筛选
        if months:
            monthly_data = [d for d in monthly_data if d['month'] in months]
        
        if not monthly_data:
            return None
        
        # 获取行业下股票数量（用于显示）
        stock_count = self.db.query(Stock).filter(
            Stock.industry_code == industry_code,
            Stock.is_delisted == 0
        ).count()
        
        base_info = {
            "industry_code": industry_code,
            "industry_name": industry_name,
            "stock_count": stock_count
        }
        
        if group_by_month and months and len(months) > 1:
            # 按月统计模式
            monthly_stats = {}
            for month in months:
                month_data = [d for d in monthly_data if d['month'] == month]
                if not month_data:
                    continue
                
                # 计算该月的统计信息
                up_count = 0
                down_count = 0
                up_pct_sum = 0.0
                down_pct_sum = 0.0
                years = set()
                
                for data in month_data:
                    years.add(data['year'])
                    pct_change = data['pct_change']
                    if pct_change > 0:
                        up_count += 1
                        up_pct_sum += pct_change
                    elif pct_change < 0:
                        down_count += 1
                        down_pct_sum += pct_change
                
                total_count = up_count + down_count
                if total_count == 0:
                    continue
                
                # 检查最小涨跌次数
                if min_total_count > 0 and total_count < min_total_count:
                    continue
                
                up_probability = (up_count / total_count * 100) if total_count > 0 else 0
                down_probability = (down_count / total_count * 100) if total_count > 0 else 0
                avg_up_pct = (up_pct_sum / up_count) if up_count > 0 else 0
                avg_down_pct = (down_pct_sum / down_count) if down_count > 0 else 0
                year_range = f"{min(years)}-{max(years)}" if years else ""
                
                monthly_stats[month] = {
                    "up_count": up_count,
                    "down_count": down_count,
                    "total_count": total_count,
                    "up_probability": round(up_probability, 2),
                    "down_probability": round(down_probability, 2),
                    "avg_up_pct": round(avg_up_pct, 2),
                    "avg_down_pct": round(avg_down_pct, 2),
                    "year_range": year_range
                }
            
            if not monthly_stats:
                return None
            
            # 计算汇总统计（所有选中月份合并）
            all_up_count = 0
            all_down_count = 0
            all_up_pct_sum = 0.0
            all_down_pct_sum = 0.0
            all_years = set()
            
            for data in monthly_data:
                all_years.add(data['year'])
                pct_change = data['pct_change']
                if pct_change > 0:
                    all_up_count += 1
                    all_up_pct_sum += pct_change
                elif pct_change < 0:
                    all_down_count += 1
                    all_down_pct_sum += pct_change
            
            all_total_count = all_up_count + all_down_count
            if all_total_count == 0:
                return None
            
            all_up_probability = (all_up_count / all_total_count * 100) if all_total_count > 0 else 0
            all_down_probability = (all_down_count / all_total_count * 100) if all_total_count > 0 else 0
            all_avg_up_pct = (all_up_pct_sum / all_up_count) if all_up_count > 0 else 0
            all_avg_down_pct = (all_down_pct_sum / all_down_count) if all_down_count > 0 else 0
            all_year_range = f"{min(all_years)}-{max(all_years)}" if all_years else ""
            
            return {
                **base_info,
                "statistics_mode": "monthly",
                "monthly_statistics": monthly_stats,
                "summary_statistics": {
                    "up_count": all_up_count,
                    "down_count": all_down_count,
                    "total_count": all_total_count,
                    "up_probability": round(all_up_probability, 2),
                    "down_probability": round(all_down_probability, 2),
                    "avg_up_pct": round(all_avg_up_pct, 2),
                    "avg_down_pct": round(all_avg_down_pct, 2),
                    "year_range": all_year_range
                }
            }
        else:
            # 汇总统计模式
            up_count = 0
            down_count = 0
            up_pct_sum = 0.0
            down_pct_sum = 0.0
            years = set()
            
            for data in monthly_data:
                years.add(data['year'])
                pct_change = data['pct_change']
                if pct_change > 0:
                    up_count += 1
                    up_pct_sum += pct_change
                elif pct_change < 0:
                    down_count += 1
                    down_pct_sum += pct_change
            
            total_count = up_count + down_count
            if total_count == 0:
                return None
            
            # 检查最小涨跌次数
            if min_total_count > 0 and total_count < min_total_count:
                return None
            
            up_probability = (up_count / total_count * 100) if total_count > 0 else 0
            down_probability = (down_count / total_count * 100) if total_count > 0 else 0
            avg_up_pct = (up_pct_sum / up_count) if up_count > 0 else 0
            avg_down_pct = (down_pct_sum / down_count) if down_count > 0 else 0
            
            year_range = f"{min(years)}-{max(years)}" if years else ""
            
            return {
                **base_info,
                "statistics_mode": "summary",
                "up_count": up_count,
                "down_count": down_count,
                "total_count": total_count,
                "up_probability": round(up_probability, 2),
                "down_probability": round(down_probability, 2),
                "avg_up_pct": round(avg_up_pct, 2),
                "avg_down_pct": round(avg_down_pct, 2),
                "year_range": year_range
            }
    
    def calculate_industries_rank_by_month(
        self,
        month: int,
        min_total_count: int = 0,
        limit: int = 20
    ) -> List[Dict]:
        """计算所有行业在指定月份的统计数据，按上涨概率排序
        
        Args:
            month: 月份（1-12）
            min_total_count: 最小总涨跌次数
            limit: 返回前N名
            
        Returns:
            按上涨概率降序排列的行业统计列表
        """
        from data_collector import DataCollector
        import time
        
        # 获取所有行业
        industries = self.get_industry_list()
        total_industries = len(industries)
        
        logger.info(f"开始查询 {total_industries} 个行业在 {month} 月的统计数据")
        
        # 创建一个共享的DataCollector实例，避免重复登录
        collector = DataCollector(self.db)
        
        results = []
        success_count = 0
        failed_count = 0
        
        for idx, industry in enumerate(industries):
            if idx % 5 == 0:  # 每5个行业记录一次进度
                logger.info(f"进度: {idx + 1}/{total_industries} - {industry['name']}")
            try:
                # 计算该行业在指定月份的统计数据，传入共享的collector
                stats = self._calculate_industry_statistics_with_collector(
                    industry_code=industry['code'],
                    months=[month],
                    min_total_count=min_total_count,
                    group_by_month=False,
                    collector=collector
                )
                
                if stats and stats.get('statistics_mode') == 'summary':
                    results.append({
                        "industry_code": stats['industry_code'],
                        "industry_name": stats['industry_name'],
                        "stock_count": stats.get('stock_count', 0),
                        "up_count": stats['up_count'],
                        "down_count": stats['down_count'],
                        "total_count": stats['total_count'],
                        "up_probability": stats['up_probability'],
                        "down_probability": stats['down_probability'],
                        "avg_up_pct": stats['avg_up_pct'],
                        "avg_down_pct": stats['avg_down_pct'],
                        "year_range": stats['year_range']
                    })
                    success_count += 1
                else:
                    failed_count += 1
                
                # 减少延迟，因为已经复用了DataCollector，不需要太多延迟
                if idx < len(industries) - 1:
                    time.sleep(0.01)  # 每个请求间隔10ms（最小延迟，避免请求过快）
                    
            except Exception as e:
                logger.warning(f"计算行业 {industry['name']} 统计失败: {e}")
                failed_count += 1
                continue
        
        logger.info(f"查询完成: 成功 {success_count} 个，失败 {failed_count} 个，共找到 {len(results)} 个有效结果")
        
        # 按上涨概率降序排序
        results.sort(key=lambda x: x['up_probability'], reverse=True)
        
        return results[:limit]
    
    def calculate_industries_rank_by_month_with_progress(
        self,
        month: int,
        min_total_count: int = 0,
        limit: int = 20,
        progress_callback=None
    ) -> List[Dict]:
        """计算所有行业在指定月份的统计数据，按上涨概率排序（带进度回调）
        
        Args:
            month: 月份（1-12）
            min_total_count: 最小总涨跌次数
            limit: 返回前N名
            progress_callback: 进度回调函数 (current, total, message)
            
        Returns:
            按上涨概率降序排列的行业统计列表
        """
        from data_collector import DataCollector
        import time
        
        # 获取所有行业
        industries = self.get_industry_list()
        total_industries = len(industries)
        
        logger.info(f"开始查询 {total_industries} 个行业在 {month} 月的统计数据")
        
        # 创建一个共享的DataCollector实例，避免重复登录
        collector = DataCollector(self.db)
        
        results = []
        success_count = 0
        failed_count = 0
        
        for idx, industry in enumerate(industries):
            if progress_callback:
                progress_callback(idx + 1, total_industries, f"正在查询 {industry['name']}...")
            
            if idx % 5 == 0:  # 每5个行业记录一次进度
                logger.info(f"进度: {idx + 1}/{total_industries} - {industry['name']}")
            
            try:
                # 使用线程实现超时机制（Windows兼容）
                import threading
                import queue as thread_queue
                
                result_queue = thread_queue.Queue()
                exception_queue = thread_queue.Queue()
                
                def query_industry():
                    try:
                        stats = self._calculate_industry_statistics_with_collector(
                            industry_code=industry['code'],
                            months=[month],
                            min_total_count=min_total_count,
                            group_by_month=False,
                            collector=collector
                        )
                        result_queue.put(stats)
                    except Exception as e:
                        exception_queue.put(e)
                
                # 启动查询线程
                query_thread = threading.Thread(target=query_industry, daemon=True)
                query_thread.start()
                query_thread.join(timeout=8)  # 8秒超时
                
                if query_thread.is_alive():
                    # 线程仍在运行，说明超时了
                    logger.warning(f"查询行业 {industry['name']} 超时（8秒），跳过")
                    failed_count += 1
                    continue
                
                # 检查是否有异常
                try:
                    exception = exception_queue.get_nowait()
                    raise exception
                except thread_queue.Empty:
                    pass
                
                # 获取结果
                try:
                    stats = result_queue.get_nowait()
                except thread_queue.Empty:
                    logger.warning(f"查询行业 {industry['name']} 未返回结果")
                    failed_count += 1
                    continue
                
                if stats and stats.get('statistics_mode') == 'summary':
                    results.append({
                        "industry_code": stats['industry_code'],
                        "industry_name": stats['industry_name'],
                        "stock_count": stats.get('stock_count', 0),
                        "up_count": stats['up_count'],
                        "down_count": stats['down_count'],
                        "total_count": stats['total_count'],
                        "up_probability": stats['up_probability'],
                        "down_probability": stats['down_probability'],
                        "avg_up_pct": stats['avg_up_pct'],
                        "avg_down_pct": stats['avg_down_pct'],
                        "year_range": stats['year_range']
                    })
                    success_count += 1
                else:
                    failed_count += 1
                
                # 减少延迟，因为已经复用了DataCollector，不需要太多延迟
                if idx < len(industries) - 1:
                    time.sleep(0.01)  # 每个请求间隔10ms（最小延迟，避免请求过快）
                    
            except Exception as e:
                logger.warning(f"计算行业 {industry['name']} 统计失败: {e}")
                failed_count += 1
                continue
        
        logger.info(f"查询完成: 总行业数 {total_industries}, 成功 {success_count} 个，失败 {failed_count} 个，共找到 {len(results)} 个有效结果")
        
        # 按上涨概率降序排序
        results.sort(key=lambda x: x['up_probability'], reverse=True)
        
        # 如果结果数量少于limit，记录日志
        if len(results) < limit:
            logger.info(f"返回结果数量 {len(results)} 少于请求的limit {limit}")
        
        return results[:limit]
    
    def _calculate_industry_statistics_with_collector(
        self,
        industry_code: str,
        months: Optional[List[int]] = None,
        min_total_count: int = 0,
        group_by_month: bool = False,
        collector=None
    ) -> Optional[Dict]:
        """计算行业统计信息（内部方法，使用传入的collector）"""
        from industry_index_mapping import get_index_code
        from datetime import datetime
        
        industry = self.db.query(Industry).filter(Industry.code == industry_code).first()
        if not industry:
            return None
        
        industry_name = industry.name
        
        # 获取行业对应的BaoStock指数代码
        index_code = get_index_code(industry_name)
        if not index_code:
            logger.warning(f"未找到行业 '{industry_name}' 对应的指数代码")
            return None
        
        # 获取行业板块的月K数据（使用传入的collector）
        start_date = "2000-01-01"
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        # 从BaoStock获取行业板块月K数据
        df = collector.get_industry_monthly_k_baostock(index_code, start_date, end_date)
        
        if df is None or len(df) == 0:
            return None
        
        # 转换为月度数据列表
        monthly_data = []
        for _, row in df.iterrows():
            if pd.notna(row.get('pct_change')):
                monthly_data.append({
                    'year': int(row['year']),
                    'month': int(row['month']),
                    'pct_change': float(row['pct_change'])
                })
        
        if not monthly_data:
            return None
        
        # 按月份筛选
        if months:
            monthly_data = [d for d in monthly_data if d['month'] in months]
        
        if not monthly_data:
            return None
        
        # 获取行业下股票数量（用于显示）
        stock_count = self.db.query(Stock).filter(
            Stock.industry_code == industry_code,
            Stock.is_delisted == 0
        ).count()
        
        base_info = {
            "industry_code": industry_code,
            "industry_name": industry_name,
            "stock_count": stock_count
        }
        
        # 汇总统计模式
        up_count = 0
        down_count = 0
        up_pct_sum = 0.0
        down_pct_sum = 0.0
        years = set()
        
        for data in monthly_data:
            years.add(data['year'])
            pct_change = data['pct_change']
            if pct_change > 0:
                up_count += 1
                up_pct_sum += pct_change
            elif pct_change < 0:
                down_count += 1
                down_pct_sum += pct_change
        
        total_count = up_count + down_count
        if total_count == 0:
            return None
        
        # 检查最小涨跌次数
        if min_total_count > 0 and total_count < min_total_count:
            return None
        
        up_probability = (up_count / total_count * 100) if total_count > 0 else 0
        down_probability = (down_count / total_count * 100) if total_count > 0 else 0
        avg_up_pct = (up_pct_sum / up_count) if up_count > 0 else 0
        avg_down_pct = (down_pct_sum / down_count) if down_count > 0 else 0
        
        year_range = f"{min(years)}-{max(years)}" if years else ""
        
        return {
            **base_info,
            "statistics_mode": "summary",
            "up_count": up_count,
            "down_count": down_count,
            "total_count": total_count,
            "up_probability": round(up_probability, 2),
            "down_probability": round(down_probability, 2),
            "avg_up_pct": round(avg_up_pct, 2),
            "avg_down_pct": round(avg_down_pct, 2),
            "year_range": year_range
        }
    
    def get_industry_list(self) -> List[Dict]:
        """获取行业列表"""
        # 如果Industry表为空，从Stock表中提取行业信息
        industry_count = self.db.query(Industry).count()
        if industry_count == 0:
            self._populate_industries_from_stocks()
        
        industries = self.db.query(Industry).order_by(Industry.level, Industry.name).all()
        
        result = []
        for industry in industries:
            result.append({
                "code": industry.code,
                "name": industry.name,
                "level": industry.level,
                "parent_code": industry.parent_code
            })
        
        return result
    
    def _populate_industries_from_stocks(self):
        """从股票数据中提取并填充行业信息"""
        try:
            # 获取所有不重复的行业名称
            stocks = self.db.query(Stock).filter(
                Stock.industry_name.isnot(None),
                Stock.industry_name != ""
            ).all()
            
            industry_map = {}  # {industry_name: industry_code}
            
            for stock in stocks:
                industry_name = stock.industry_name.strip()
                if not industry_name:
                    continue
                
                # 生成行业代码（使用行业名称的hash值，确保唯一性）
                import hashlib
                industry_code = hashlib.md5(industry_name.encode('utf-8')).hexdigest()[:16]
                
                # 如果行业不存在，创建它
                if industry_name not in industry_map:
                    existing_industry = self.db.query(Industry).filter(
                        Industry.code == industry_code
                    ).first()
                    
                    if not existing_industry:
                        industry = Industry(
                            code=industry_code,
                            name=industry_name,
                            level=1,
                            parent_code=None,
                            classification="custom"
                        )
                        self.db.add(industry)
                    
                    industry_map[industry_name] = industry_code
                
                # 更新股票的industry_code
                stock.industry_code = industry_map[industry_name]
            
            self.db.commit()
            logger.info(f"从股票数据中提取了 {len(industry_map)} 个行业")
        except Exception as e:
            logger.error(f"填充行业数据失败: {e}", exc_info=True)
            self.db.rollback()
    
    def get_stock_suggestions(self, keyword: str, limit: int = 10) -> List[Dict]:
        """获取股票代码/名称自动补全建议"""
        stocks = self.db.query(Stock).filter(
            or_(
                Stock.code.like(f"%{keyword}%"),
                Stock.name.like(f"%{keyword}%")
            ),
            Stock.is_delisted == 0
        ).limit(limit).all()
        
        result = []
        for stock in stocks:
            result.append({
                "code": stock.code,
                "name": stock.name,
                "market": stock.market
            })
        
        return result


