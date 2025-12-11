"""
主程序入口
"""
from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates
import asyncio
import json
from sqlalchemy.orm import Session
from database import engine, get_db, Base
from models import Stock, MonthlyKData
from data_collector import DataCollector
from statistics import StatisticsCalculator
from config import WEB_CONFIG, DATA_SOURCE_CONFIG, STATISTICS_CONFIG, save_data_source_config
import uvicorn
from typing import List, Optional
from pydantic import BaseModel
import pandas as pd
from datetime import datetime
import os
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_analysis.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 创建数据库表
Base.metadata.create_all(bind=engine)

app = FastAPI(title="股票月K统计分析系统")

# 静态文件和模板
if not os.path.exists("static"):
    os.makedirs("static")
if not os.path.exists("templates"):
    os.makedirs("templates")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# Pydantic模型
class StockQuery(BaseModel):
    stock_code: str
    months: Optional[List[int]] = None
    min_total_count: int = 0  # 最小总涨跌次数
    group_by_month: bool = False  # True=按月统计，False=汇总统计


class BatchQuery(BaseModel):
    months: Optional[List[int]] = None
    market: Optional[str] = None
    industry_code: Optional[str] = None
    min_total_count: int = 0  # 最小总涨跌次数
    limit: int = 20
    order_by: str = "up_probability"


class IndustryQuery(BaseModel):
    industry_code: str
    months: Optional[List[int]] = None
    min_total_count: int = 0
    group_by_month: bool = False  # True=按月统计，False=汇总统计  # 最小总涨跌次数


class IndustryRankQuery(BaseModel):
    month: int  # 月份（1-12）
    min_total_count: int = 0  # 最小总涨跌次数
    limit: int = 20  # 返回前N名


class UpdateRequest(BaseModel):
    stock_codes: Optional[List[str]] = None  # None表示更新所有股票
    force_update: bool = False


class ConfigUpdate(BaseModel):
    baostock_enabled: bool = True
    tushare_enabled: bool = False
    tushare_token: Optional[str] = None
    finnhub_enabled: bool = False
    finnhub_api_key: Optional[str] = None
    akshare_enabled: bool = False
    akshare_api_key: Optional[str] = None  # 保留字段，AKShare不需要API key


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """首页"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/stocks/suggest")
async def get_stock_suggestions(keyword: str, db: Session = Depends(get_db)):
    """股票代码/名称自动补全"""
    calculator = StatisticsCalculator(db)
    suggestions = calculator.get_stock_suggestions(keyword, limit=10)
    return {"suggestions": suggestions}


@app.post("/api/stocks/statistics")
async def get_stock_statistics(query: StockQuery, db: Session = Depends(get_db)):
    """获取单只股票统计信息"""
    calculator = StatisticsCalculator(db)
    result = calculator.calculate_stock_statistics(
        query.stock_code,
        months=query.months,
        min_total_count=query.min_total_count,
        group_by_month=query.group_by_month
    )
    
    if not result:
        raise HTTPException(status_code=404, detail="未找到统计数据")
    
    return result


@app.post("/api/stocks/batch")
async def get_batch_statistics(query: BatchQuery, db: Session = Depends(get_db)):
    """批量获取股票统计信息"""
    calculator = StatisticsCalculator(db)
    results = calculator.calculate_batch_statistics(
        months=query.months,
        market=query.market,
        industry_code=query.industry_code,
        min_total_count=query.min_total_count,
        exclude_delisted=STATISTICS_CONFIG["exclude_delisted"],
        limit=query.limit,
        order_by=query.order_by
    )
    
    return {"results": results, "count": len(results)}


@app.get("/api/industries")
async def get_industries(db: Session = Depends(get_db)):
    """获取行业列表"""
    calculator = StatisticsCalculator(db)
    industries = calculator.get_industry_list()
    return {"industries": industries}


@app.post("/api/industries/statistics")
async def get_industry_statistics(query: IndustryQuery, db: Session = Depends(get_db)):
    """获取行业统计信息"""
    calculator = StatisticsCalculator(db)
    result = calculator.calculate_industry_statistics(
        query.industry_code,
        months=query.months,
        min_total_count=query.min_total_count,
        group_by_month=query.group_by_month
    )
    
    if not result:
        raise HTTPException(status_code=404, detail="未找到行业统计数据")
    
    return result


@app.post("/api/industries/rank-by-month")
async def get_industries_rank_by_month(query: IndustryRankQuery, db: Session = Depends(get_db)):
    """获取所有行业在指定月份的上涨概率排名（使用SSE流式返回）"""
    if not (1 <= query.month <= 12):
        raise HTTPException(status_code=400, detail="月份必须在1-12之间")
    
    async def generate_progress():
        import queue
        import threading
        import json
        
        progress_queue = queue.Queue()
        update_complete = threading.Event()
        
        def progress_callback(current, total, message):
            progress_data = {
                "current": current,
                "total": total,
                "message": message,
                "percent": int(current / total * 100) if total > 0 else 0
            }
            progress_queue.put(progress_data)
        
        def run_query():
            try:
                # 在后台线程中创建新的数据库会话
                from database import SessionLocal
                thread_db = SessionLocal()
                thread_calculator = StatisticsCalculator(thread_db)
                
                try:
                    # 获取所有行业
                    industries = thread_calculator.get_industry_list()
                    total_industries = len(industries)
                    
                    progress_callback(0, total_industries, f"开始查询 {total_industries} 个行业...")
                    
                    # 调用带进度回调的查询方法
                    results = thread_calculator.calculate_industries_rank_by_month_with_progress(
                        month=query.month,
                        min_total_count=query.min_total_count,
                        limit=query.limit,
                        progress_callback=progress_callback
                    )
                    
                    # 获取所有行业列表，用于统计
                    all_industries = thread_calculator.get_industry_list()
                    total_industries = len(all_industries)
                    
                    # 发送最终结果（包含统计信息）
                    final_data = {
                        "done": True,
                        "month": query.month,
                        "results": results,
                        "count": len(results),
                        "total_industries": total_industries,
                        "success_count": len(results),
                        "failed_count": total_industries - len(results)
                    }
                    progress_queue.put(final_data)
                    
                finally:
                    thread_db.close()
                    
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"查询行业排名失败: {e}", exc_info=True)
                error_data = {
                    "error": True,
                    "message": f"查询失败: {str(e)}"
                }
                progress_queue.put(error_data)
            finally:
                update_complete.set()
        
        try:
            # 发送初始消息
            yield f"data: {json.dumps({'current': 0, 'total': 100, 'message': '开始查询行业排名...', 'percent': 0}, ensure_ascii=False)}\n\n"
            
            # 在后台线程中运行查询任务
            import threading
            query_thread = threading.Thread(target=run_query, daemon=True)
            query_thread.start()
            
            # 持续发送进度更新
            last_percent = 0
            final_result = None
            
            while not update_complete.is_set() or not progress_queue.empty():
                try:
                    # 检查是否有进度更新（非阻塞）
                    try:
                        while True:
                            progress_data = progress_queue.get_nowait()
                            
                            # 检查是否是最终结果
                            if progress_data.get("done"):
                                final_result = progress_data
                                yield f"data: {json.dumps(progress_data, ensure_ascii=False)}\n\n"
                                break
                            elif progress_data.get("error"):
                                yield f"data: {json.dumps(progress_data, ensure_ascii=False)}\n\n"
                                break
                            else:
                                percent = progress_data.get("percent", 0)
                                if percent > last_percent or progress_data.get("message", ""):
                                    yield f"data: {json.dumps(progress_data, ensure_ascii=False)}\n\n"
                                    last_percent = percent
                    except queue.Empty:
                        pass
                    
                    # 如果已经收到最终结果，退出循环
                    if final_result:
                        break
                    
                    # 短暂休眠，避免CPU占用过高
                    import asyncio
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    import logging
                    logger = logging.getLogger(__name__)
                    logger.error(f"发送进度更新时发生错误: {e}", exc_info=True)
                    break
            
            # 确保最终结果已发送
            if final_result:
                yield f"data: {json.dumps(final_result, ensure_ascii=False)}\n\n"
                
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"生成进度流时发生错误: {e}", exc_info=True)
            error_data = {
                "error": True,
                "message": f"查询失败: {str(e)}"
            }
            yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"
    
    return StreamingResponse(
        generate_progress(), 
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@app.post("/api/data/update")
async def update_data(request: UpdateRequest, db: Session = Depends(get_db)):
    """更新数据"""
    try:
        collector = DataCollector(db)
        
        if request.stock_codes:
            # 更新指定股票
            total_count = 0
            success_count = 0
            failed_count = 0
            for code in request.stock_codes:
                try:
                    count = collector.update_monthly_k_data(code, force_update=request.force_update)
                    total_count += count
                    success_count += 1
                except Exception as e:
                    failed_count += 1
                    logger.error(f"更新股票 {code} 失败: {e}")
            
            collector._logout_baostock()  # 登出BaoStock
            
            return {
                "message": f"更新完成，成功：{success_count}，失败：{failed_count}，共更新 {total_count} 条记录"
            }
        else:
            # 更新所有股票 - 使用流式响应返回进度
            async def generate_progress():
                import queue
                import concurrent.futures
                
                progress_queue = queue.Queue()
                
                def progress_callback(current, total, message):
                    progress_data = {
                        "current": current,
                        "total": total,
                        "message": message,
                        "percent": int(current / total * 100) if total > 0 else 0
                    }
                    progress_queue.put(progress_data)
                
                def run_update():
                    # 在后台线程中创建新的数据库会话
                    from database import SessionLocal
                    thread_db = SessionLocal()
                    thread_collector = DataCollector(thread_db)
                    
                    try:
                        # 先更新股票列表
                        stock_count = thread_collector.update_stock_list(progress_callback=progress_callback)
                        
                        # 获取所有股票代码和名称（在会话内获取，避免会话问题）
                        stocks = thread_db.query(Stock).filter(Stock.is_delisted == 0).all()
                        # 立即提取所有需要的数据，避免后续访问时会话已关闭
                        stock_list = [(s.code, s.name if s.name else s.code) for s in stocks]
                        total_stocks = len(stock_list)
                        total_count = 0
                        success_count = 0
                        failed_count = 0
                        
                        # 关闭查询会话，后续使用新的会话
                        thread_db.close()
                        
                        # 使用批量会话，每50个股票重新创建一次会话，避免会话过期
                        batch_size = 50
                        current_db = None
                        current_collector = None
                        
                        for idx, (stock_code, stock_name) in enumerate(stock_list):
                            try:
                                # 每50个股票或第一个股票时创建新会话
                                if idx % batch_size == 0:
                                    if current_db:
                                        try:
                                            current_collector._logout_baostock()
                                            current_db.close()
                                        except:
                                            pass
                                    current_db = SessionLocal()
                                    current_collector = DataCollector(current_db)
                                
                                def stock_progress(current, total, message):
                                    # 计算总体进度：股票列表更新占10%，月K数据更新占90%
                                    overall_current = 10 + int((idx + current / total) / total_stocks * 90) if total > 0 else 10 + int(idx / total_stocks * 90)
                                    progress_queue.put({
                                        "current": overall_current,
                                        "total": 100,
                                        "message": f"正在更新 {stock_code} - {stock_name} ({idx + 1}/{total_stocks})",
                                        "percent": overall_current
                                    })
                                
                                count = current_collector.update_monthly_k_data(stock_code, force_update=request.force_update, progress_callback=stock_progress)
                                total_count += count
                                success_count += 1
                            except Exception as e:
                                failed_count += 1
                                logger.error(f"更新股票 {stock_code} 失败: {e}", exc_info=True)
                        
                        # 关闭最后一个会话
                        if current_db:
                            try:
                                current_collector._logout_baostock()
                                current_db.close()
                            except:
                                pass
                        
                        thread_collector._logout_baostock()
                        return stock_count, total_count, success_count, failed_count, None
                    except Exception as e:
                        logger.error(f"更新过程出错: {e}", exc_info=True)
                        try:
                            thread_db.close()
                        except:
                            pass
                        return 0, 0, 0, 0, str(e)
                
                try:
                    loop = asyncio.get_event_loop()
                    future = loop.run_in_executor(None, run_update)
                    
                    while True:
                        try:
                            while not progress_queue.empty():
                                progress_data = progress_queue.get_nowait()
                                yield f"data: {json.dumps(progress_data, ensure_ascii=False)}\n\n"
                            
                            if future.done():
                                stock_count, total_count, success_count, failed_count, error = await future
                                
                                # 发送剩余的进度更新
                                while not progress_queue.empty():
                                    progress_data = progress_queue.get_nowait()
                                    yield f"data: {json.dumps(progress_data, ensure_ascii=False)}\n\n"
                                
                                if error:
                                    error_data = {
                                        "current": 0,
                                        "total": 100,
                                        "message": f"更新失败: {error}",
                                        "percent": 0,
                                        "error": True
                                    }
                                    yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"
                                else:
                                    final_data = {
                                        "current": 100,
                                        "total": 100,
                                        "message": f"更新完成，股票列表：{stock_count}，成功：{success_count}，失败：{failed_count}，月K数据：{total_count} 条记录",
                                        "percent": 100,
                                        "done": True
                                    }
                                    yield f"data: {json.dumps(final_data, ensure_ascii=False)}\n\n"
                                break
                            
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            logger.error(f"发送进度时出错: {e}")
                            break
                except Exception as e:
                    logger.error(f"更新月K数据时发生错误: {e}", exc_info=True)
                    error_data = {
                        "current": 0,
                        "total": 100,
                        "message": f"更新失败: {str(e)}",
                        "percent": 0,
                        "error": True
                    }
                    yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"
            
            return StreamingResponse(generate_progress(), media_type="text/event-stream")
    except Exception as e:
        logger.error(f"更新数据时发生错误: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"更新失败: {str(e)}")


@app.post("/api/data/update-stock-list")
async def update_stock_list(db: Session = Depends(get_db)):
    """更新股票列表（带进度反馈）"""
    async def generate_progress():
        import queue
        import concurrent.futures
        import threading
        
        progress_queue = queue.Queue()
        update_complete = threading.Event()
        update_result = {"count": 0, "error": None}
        
        def progress_callback(current, total, message):
            try:
                progress_data = {
                    "current": current,
                    "total": total,
                    "message": message,
                    "percent": int(current / total * 100) if total > 0 else 0
                }
                progress_queue.put(progress_data)
            except Exception as e:
                logger.error(f"进度回调出错: {e}")
        
        def run_update():
            # 在后台线程中创建新的数据库会话
            from database import SessionLocal
            thread_db = SessionLocal()
            thread_collector = DataCollector(thread_db)
            
            try:
                count = thread_collector.update_stock_list(progress_callback=progress_callback)
                thread_collector._logout_baostock()
                update_result["count"] = count
                update_result["error"] = None
            except Exception as e:
                logger.error(f"更新股票列表失败: {e}", exc_info=True)
                update_result["error"] = str(e)
            finally:
                try:
                    thread_db.close()
                except:
                    pass
                update_complete.set()
        
        try:
            # 发送初始消息
            yield f"data: {json.dumps({'current': 0, 'total': 100, 'message': '开始更新股票列表...', 'percent': 0}, ensure_ascii=False)}\n\n"
            
            # 在后台线程中运行更新任务
            import threading
            update_thread = threading.Thread(target=run_update, daemon=True)
            update_thread.start()
            
            # 持续发送进度更新
            last_percent = 0
            while not update_complete.is_set() or not progress_queue.empty():
                try:
                    # 检查是否有进度更新（非阻塞）
                    try:
                        while True:
                            progress_data = progress_queue.get_nowait()
                            percent = progress_data.get("percent", 0)
                            if percent > last_percent or progress_data.get("message", ""):
                                yield f"data: {json.dumps(progress_data, ensure_ascii=False)}\n\n"
                                last_percent = percent
                    except queue.Empty:
                        pass
                    
                    # 如果任务完成，发送最终消息
                    if update_complete.is_set():
                        # 发送剩余的进度更新
                        try:
                            while True:
                                progress_data = progress_queue.get_nowait()
                                yield f"data: {json.dumps(progress_data, ensure_ascii=False)}\n\n"
                        except queue.Empty:
                            pass
                        
                        if update_result["error"]:
                            error_data = {
                                "current": 0,
                                "total": 100,
                                "message": f"更新失败: {update_result['error']}",
                                "percent": 0,
                                "error": True
                            }
                            yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"
                        else:
                            final_data = {
                                "current": 100,
                                "total": 100,
                                "message": f"更新完成，共 {update_result['count']} 只股票",
                                "percent": 100,
                                "done": True
                            }
                            yield f"data: {json.dumps(final_data, ensure_ascii=False)}\n\n"
                        break
                    
                    await asyncio.sleep(0.2)  # 短暂等待，避免CPU占用过高
                except Exception as e:
                    logger.error(f"发送进度时出错: {e}", exc_info=True)
                    break
        except Exception as e:
            logger.error(f"更新股票列表时发生错误: {e}", exc_info=True)
            error_data = {
                "current": 0,
                "total": 100,
                "message": f"更新失败: {str(e)}",
                "percent": 0,
                "error": True
            }
            yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"
    
    return StreamingResponse(
        generate_progress(), 
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


def mask_api_key(api_key: str, show_chars: int = 4) -> str:
    """掩码API密钥，只显示前几位
    
    Args:
        api_key: 原始API密钥
        show_chars: 显示前几位字符
        
    Returns:
        掩码后的API密钥（例如：abcd****）
    """
    if not api_key:
        return None
    if len(api_key) <= show_chars:
        return "*" * len(api_key)
    return api_key[:show_chars] + "*" * (len(api_key) - show_chars)

@app.get("/api/config")
async def get_config():
    """获取配置（API密钥已掩码，不返回明文）"""
    # 返回配置，API密钥已掩码处理
    safe_config = {
        "primary": DATA_SOURCE_CONFIG["primary"],
        "backup": DATA_SOURCE_CONFIG["backup"],
        "baostock": {
            "enabled": DATA_SOURCE_CONFIG["baostock"]["enabled"],
            "api_key": mask_api_key(DATA_SOURCE_CONFIG["baostock"]["api_key"]) if DATA_SOURCE_CONFIG["baostock"]["api_key"] else None
        },
        "tushare": {
            "enabled": DATA_SOURCE_CONFIG["tushare"]["enabled"],
            "api_key": mask_api_key(DATA_SOURCE_CONFIG["tushare"]["api_key"]) if DATA_SOURCE_CONFIG["tushare"]["api_key"] else None
        },
        "finnhub": {
            "enabled": DATA_SOURCE_CONFIG["finnhub"]["enabled"],
            "api_key": mask_api_key(DATA_SOURCE_CONFIG["finnhub"]["api_key"]) if DATA_SOURCE_CONFIG["finnhub"]["api_key"] else None
        },
        "akshare": {
            "enabled": DATA_SOURCE_CONFIG.get("akshare", {}).get("enabled", False),
            "api_key": mask_api_key(DATA_SOURCE_CONFIG.get("akshare", {}).get("api_key")) if DATA_SOURCE_CONFIG.get("akshare", {}).get("api_key") else None
        }
    }
    return {
        "data_source": safe_config,
        "statistics": STATISTICS_CONFIG
    }


@app.post("/api/config")
async def update_config(config: ConfigUpdate):
    """更新配置"""
    DATA_SOURCE_CONFIG["baostock"]["enabled"] = config.baostock_enabled
    DATA_SOURCE_CONFIG["tushare"]["enabled"] = config.tushare_enabled
    if config.tushare_token:
        DATA_SOURCE_CONFIG["tushare"]["api_key"] = config.tushare_token
    elif config.tushare_token == "":
        # 如果传入空字符串，清空token
        DATA_SOURCE_CONFIG["tushare"]["api_key"] = None
    DATA_SOURCE_CONFIG["finnhub"]["enabled"] = config.finnhub_enabled
    if config.finnhub_api_key:
        DATA_SOURCE_CONFIG["finnhub"]["api_key"] = config.finnhub_api_key
    elif config.finnhub_api_key == "":
        # 如果传入空字符串，清空API key
        DATA_SOURCE_CONFIG["finnhub"]["api_key"] = None
    
    # 确保akshare配置存在
    if "akshare" not in DATA_SOURCE_CONFIG:
        DATA_SOURCE_CONFIG["akshare"] = {"enabled": False, "api_key": None}
    DATA_SOURCE_CONFIG["akshare"]["enabled"] = config.akshare_enabled
    
    # 保存配置到文件
    save_data_source_config(DATA_SOURCE_CONFIG)
    
    return {"message": "配置更新成功"}


@app.post("/api/config/test-connection")
async def test_connection(config: ConfigUpdate):
    """测试数据源连接"""
    results = {
        "baostock": {"success": False, "message": ""},
        "tushare": {"success": False, "message": ""},
        "finnhub": {"success": False, "message": ""},
        "akshare": {"success": False, "message": ""}
    }
    
    # 临时更新配置用于测试
    original_config = {
        "baostock_enabled": DATA_SOURCE_CONFIG["baostock"]["enabled"],
        "tushare_enabled": DATA_SOURCE_CONFIG["tushare"]["enabled"],
        "tushare_api_key": DATA_SOURCE_CONFIG["tushare"]["api_key"],
        "finnhub_enabled": DATA_SOURCE_CONFIG["finnhub"]["enabled"],
        "finnhub_api_key": DATA_SOURCE_CONFIG["finnhub"]["api_key"],
        "akshare_enabled": DATA_SOURCE_CONFIG.get("akshare", {}).get("enabled", False)
    }
    
    # 应用测试配置
    DATA_SOURCE_CONFIG["baostock"]["enabled"] = config.baostock_enabled
    DATA_SOURCE_CONFIG["tushare"]["enabled"] = config.tushare_enabled
    if config.tushare_token:
        DATA_SOURCE_CONFIG["tushare"]["api_key"] = config.tushare_token
    DATA_SOURCE_CONFIG["finnhub"]["enabled"] = config.finnhub_enabled
    if config.finnhub_api_key:
        DATA_SOURCE_CONFIG["finnhub"]["api_key"] = config.finnhub_api_key
    
    # 测试BaoStock
    if config.baostock_enabled:
        try:
            import baostock as bs
            result = bs.login()
            if result.error_code == '0':
                results["baostock"]["success"] = True
                results["baostock"]["message"] = "连接成功"
                bs.logout()
            else:
                results["baostock"]["message"] = f"连接失败: {result.error_msg}"
        except Exception as e:
            results["baostock"]["message"] = f"连接失败: {str(e)}"
    else:
        results["baostock"]["message"] = "未启用"
    
    # 测试tushare
    if config.tushare_enabled:
        try:
            import tushare as ts
            if not config.tushare_token:
                results["tushare"]["message"] = "未配置API token"
            else:
                ts.set_token(config.tushare_token)
                pro = ts.pro_api()
                # 尝试获取股票基本信息来测试连接
                df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name', limit=1)
                if df is not None and len(df) > 0:
                    results["tushare"]["success"] = True
                    results["tushare"]["message"] = f"连接成功，测试获取到 {len(df)} 条数据"
                else:
                    results["tushare"]["message"] = "连接成功，但未获取到数据"
        except Exception as e:
            error_msg = str(e)
            if "token" in error_msg.lower() or "401" in error_msg:
                results["tushare"]["message"] = "API token无效或已过期"
            else:
                results["tushare"]["message"] = f"连接失败: {error_msg}"
    else:
        results["tushare"]["message"] = "未启用"
    
    # 测试AKShare
    if config.akshare_enabled:
        try:
            import akshare as ak
            # 尝试获取股票基本信息来测试连接
            df = ak.stock_info_a_code_name()
            if df is not None and len(df) > 0:
                results["akshare"]["success"] = True
                results["akshare"]["message"] = f"连接成功，测试获取到 {len(df)} 条数据"
            else:
                results["akshare"]["message"] = "连接成功，但未获取到数据"
        except ImportError:
            results["akshare"]["message"] = "AKShare未安装，请运行: pip install akshare"
        except Exception as e:
            error_msg = str(e)
            if "proxy" in error_msg.lower() or "connection" in error_msg.lower():
                results["akshare"]["message"] = "网络连接失败，可能是代理设置问题。请检查网络环境。"
            else:
                results["akshare"]["message"] = f"连接失败: {error_msg[:100]}"
    else:
        results["akshare"]["message"] = "未启用"
    
    # 测试FinnHub
    if config.finnhub_enabled:
        try:
            import requests
            if not config.finnhub_api_key:
                results["finnhub"]["message"] = "未配置API key"
            else:
                # 测试API连接（使用股票列表接口）
                url = f"https://finnhub.io/api/v1/stock/symbol?exchange=US&token={config.finnhub_api_key}"
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if isinstance(data, list) and len(data) > 0:
                        results["finnhub"]["success"] = True
                        results["finnhub"]["message"] = f"连接成功，测试获取到 {len(data)} 条数据"
                    else:
                        results["finnhub"]["message"] = "连接成功，但未获取到数据"
                elif response.status_code == 401:
                    results["finnhub"]["message"] = "API key无效或已过期"
                else:
                    results["finnhub"]["message"] = f"连接失败: HTTP {response.status_code}"
        except requests.exceptions.Timeout:
            results["finnhub"]["message"] = "连接超时"
        except Exception as e:
            results["finnhub"]["message"] = f"连接失败: {str(e)}"
    else:
        results["finnhub"]["message"] = "未启用"
    
    # 恢复原始配置
    DATA_SOURCE_CONFIG["baostock"]["enabled"] = original_config["baostock_enabled"]
    DATA_SOURCE_CONFIG["tushare"]["enabled"] = original_config["tushare_enabled"]
    DATA_SOURCE_CONFIG["tushare"]["api_key"] = original_config["tushare_api_key"]
    DATA_SOURCE_CONFIG["finnhub"]["enabled"] = original_config["finnhub_enabled"]
    DATA_SOURCE_CONFIG["finnhub"]["api_key"] = original_config["finnhub_api_key"]
    if "akshare" in DATA_SOURCE_CONFIG:
        DATA_SOURCE_CONFIG["akshare"]["enabled"] = original_config["akshare_enabled"]
    
    return results


@app.post("/api/export/excel")
async def export_to_excel(query: BatchQuery, db: Session = Depends(get_db)):
    """导出Excel"""
    calculator = StatisticsCalculator(db)
    # 导出时使用查询时的limit参数，导出与查询显示一致的数据量
    results = calculator.calculate_batch_statistics(
        months=query.months,
        market=query.market,
        industry_code=query.industry_code,
        min_total_count=query.min_total_count,
        exclude_delisted=STATISTICS_CONFIG["exclude_delisted"],
        limit=query.limit,  # 使用查询时的limit，导出与显示一致的数据量
        order_by=query.order_by
    )
    
    if not results:
        raise HTTPException(status_code=404, detail="没有可导出的数据")
    
    # 确保 static 目录存在
    os.makedirs("static", exist_ok=True)
    
    df = pd.DataFrame(results)
    
    # 定义中文列名映射
    column_mapping = {
        'rank': '排名',
        'stock_code': '股票代码',
        'stock_name': '股票名称',
        'market': '市场',
        'listing_date': '上市日期',
        'up_count': '上涨次数',
        'down_count': '下跌次数',
        'total_count': '总涨跌次数',
        'up_probability': '上涨概率(%)',
        'down_probability': '下跌概率(%)',
        'avg_up_pct': '平均涨幅(%)',
        'avg_down_pct': '平均跌幅(%)',
        'year_range': '统计年份范围',
        'years_count': '统计年数'
    }
    
    # 重命名列
    df = df.rename(columns=column_mapping)
    
    # 重新排列列的顺序
    column_order = ['排名', '股票代码', '股票名称', '市场', '上市日期', 
                    '上涨次数', '下跌次数', '总涨跌次数', '上涨概率(%)', '下跌概率(%)',
                    '平均涨幅(%)', '平均跌幅(%)', '统计年份范围', '统计年数']
    # 只保留存在的列
    column_order = [col for col in column_order if col in df.columns]
    df = df[column_order]
    
    # 生成文件名（使用连字符而不是下划线，避免格式问题）
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    filename = f"股票统计_{timestamp}.xlsx"
    filepath = os.path.abspath(os.path.join("static", filename))
    
    # 导出Excel，使用openpyxl引擎
    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='股票统计')
    
    return FileResponse(
        filepath, 
        filename=filename,
        media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@app.post("/api/export/csv")
async def export_to_csv(query: BatchQuery, db: Session = Depends(get_db)):
    """导出CSV"""
    calculator = StatisticsCalculator(db)
    # 导出时使用查询时的limit参数，导出与查询显示一致的数据量
    results = calculator.calculate_batch_statistics(
        months=query.months,
        market=query.market,
        industry_code=query.industry_code,
        min_total_count=query.min_total_count,
        exclude_delisted=STATISTICS_CONFIG["exclude_delisted"],
        limit=query.limit,  # 使用查询时的limit，导出与显示一致的数据量
        order_by=query.order_by
    )
    
    if not results:
        raise HTTPException(status_code=404, detail="没有可导出的数据")
    
    # 确保 static 目录存在
    os.makedirs("static", exist_ok=True)
    
    df = pd.DataFrame(results)
    
    # 定义中文列名映射
    column_mapping = {
        'rank': '排名',
        'stock_code': '股票代码',
        'stock_name': '股票名称',
        'market': '市场',
        'listing_date': '上市日期',
        'up_count': '上涨次数',
        'down_count': '下跌次数',
        'total_count': '总涨跌次数',
        'up_probability': '上涨概率(%)',
        'down_probability': '下跌概率(%)',
        'avg_up_pct': '平均涨幅(%)',
        'avg_down_pct': '平均跌幅(%)',
        'year_range': '统计年份范围',
        'years_count': '统计年数'
    }
    
    # 重命名列
    df = df.rename(columns=column_mapping)
    
    # 重新排列列的顺序
    column_order = ['排名', '股票代码', '股票名称', '市场', '上市日期', 
                    '上涨次数', '下跌次数', '总涨跌次数', '上涨概率(%)', '下跌概率(%)',
                    '平均涨幅(%)', '平均跌幅(%)', '统计年份范围', '统计年数']
    # 只保留存在的列
    column_order = [col for col in column_order if col in df.columns]
    df = df[column_order]
    
    # 生成文件名（使用连字符而不是下划线，避免格式问题）
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
    filename = f"股票统计_{timestamp}.csv"
    filepath = os.path.abspath(os.path.join("static", filename))
    
    # 导出CSV，使用UTF-8-BOM编码以支持Excel正确显示中文
    # 使用utf-8-sig编码（带BOM的UTF-8），确保Excel能正确识别中文
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    
    return FileResponse(
        filepath, 
        filename=filename,
        media_type='text/csv; charset=utf-8'
    )


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=WEB_CONFIG["host"],
        port=WEB_CONFIG["port"],
        reload=False  # 禁用自动重载，避免文件变化导致服务重启
    )

