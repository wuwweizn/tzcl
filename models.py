"""
数据库模型定义
"""
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, Text, ForeignKey, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from database import Base


class Stock(Base):
    """股票基本信息表"""
    __tablename__ = "stocks"
    
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(10), unique=True, index=True, nullable=False, comment="股票代码")
    name = Column(String(50), nullable=False, comment="股票名称")
    market = Column(String(10), nullable=False, comment="市场：sh/sz")
    listing_date = Column(Date, nullable=False, comment="上市日期")
    industry_code = Column(String(50), index=True, comment="行业代码")
    industry_name = Column(String(100), comment="行业名称")
    industry_level = Column(Integer, default=1, comment="行业级别：1/2/3")
    is_st = Column(Integer, default=0, comment="是否ST：0否1是")
    is_delisted = Column(Integer, default=0, comment="是否退市：0否1是")
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # 关联关系
    monthly_data = relationship("MonthlyKData", back_populates="stock", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_code', 'code'),
        Index('idx_industry', 'industry_code'),
    )


class MonthlyKData(Base):
    """前复权月K数据表"""
    __tablename__ = "monthly_k_data"
    
    id = Column(Integer, primary_key=True, index=True)
    stock_code = Column(String(10), ForeignKey("stocks.code"), nullable=False, index=True, comment="股票代码")
    year = Column(Integer, nullable=False, index=True, comment="年份")
    month = Column(Integer, nullable=False, index=True, comment="月份")
    open_price = Column(Float, comment="开盘价（前复权）")
    close_price = Column(Float, nullable=False, comment="收盘价（前复权）")
    high_price = Column(Float, comment="最高价（前复权）")
    low_price = Column(Float, comment="最低价（前复权）")
    volume = Column(Float, comment="成交量")
    amount = Column(Float, comment="成交额")
    pct_change = Column(Float, comment="涨跌幅（%）")
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # 关联关系
    stock = relationship("Stock", back_populates="monthly_data")
    
    __table_args__ = (
        Index('idx_stock_year_month', 'stock_code', 'year', 'month', unique=True),
        Index('idx_year_month', 'year', 'month'),
    )


class Industry(Base):
    """行业分类表"""
    __tablename__ = "industries"
    
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(50), unique=True, index=True, nullable=False, comment="行业代码")
    name = Column(String(100), nullable=False, comment="行业名称")
    level = Column(Integer, default=1, comment="行业级别：1/2/3")
    parent_code = Column(String(50), ForeignKey("industries.code"), nullable=True, comment="父行业代码")
    classification = Column(String(50), default="sw", comment="分类标准：sw申万/zh中信等")
    created_at = Column(DateTime, default=datetime.now)
    
    # 自关联关系
    parent = relationship("Industry", remote_side=[code], backref="children")


class StatisticsCache(Base):
    """统计结果缓存表（可选，用于提升性能）"""
    __tablename__ = "statistics_cache"
    
    id = Column(Integer, primary_key=True, index=True)
    cache_key = Column(String(200), unique=True, index=True, nullable=False, comment="缓存键")
    cache_type = Column(String(50), nullable=False, comment="缓存类型：stock/industry/month")
    stock_code = Column(String(10), index=True, comment="股票代码")
    industry_code = Column(String(50), index=True, comment="行业代码")
    months = Column(String(100), comment="筛选的月份，如：1,2,3")
    up_count = Column(Integer, default=0, comment="上涨次数")
    down_count = Column(Integer, default=0, comment="下跌次数")
    up_probability = Column(Float, comment="上涨概率")
    down_probability = Column(Float, comment="下跌概率")
    avg_up_pct = Column(Float, comment="平均涨幅")
    avg_down_pct = Column(Float, comment="平均跌幅")
    year_range = Column(String(20), comment="统计年份范围，如：2008-2025")
    result_data = Column(Text, comment="JSON格式的详细结果")
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    __table_args__ = (
        Index('idx_cache_key', 'cache_key'),
    )


