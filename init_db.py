"""
初始化数据库脚本
"""
from database import engine, Base
from models import Stock, MonthlyKData, Industry, StatisticsCache

def init_database():
    """初始化数据库，创建所有表"""
    print("正在创建数据库表...")
    Base.metadata.create_all(bind=engine)
    print("数据库初始化完成！")

if __name__ == "__main__":
    init_database()


