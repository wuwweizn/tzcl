"""
配置文件
"""
import os
import json
from pathlib import Path
from crypto_utils import encrypt_api_key, decrypt_api_key, is_encrypted

# 项目根目录
BASE_DIR = Path(__file__).parent

# 数据库路径
DATABASE_URL = f"sqlite:///{BASE_DIR}/stock_analysis.db"

# 配置文件路径
CONFIG_FILE = BASE_DIR / "data_source_config.json"

# 默认数据源配置
DEFAULT_DATA_SOURCE_CONFIG = {
    "primary": "baostock",  # 优先使用BaoStock
    "backup": ["tushare", "finnhub"],  # 备用数据源
    "baostock": {
        "enabled": True,
        "api_key": None,  # BaoStock不需要API key
    },
    "tushare": {
        "enabled": False,
        "api_key": None,  # 需要配置token
    },
    "finnhub": {
        "enabled": False,
        "api_key": None,  # 需要配置API key
    },
    "akshare": {
        "enabled": False,
        "api_key": None,  # AKShare不需要API key，但保留字段以保持一致性
    }
}

# 加载配置
def load_data_source_config():
    """从文件加载数据源配置（自动解密API密钥）"""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                config = json.load(f)
                # 合并默认配置，确保所有字段都存在
                result = DEFAULT_DATA_SOURCE_CONFIG.copy()
                result.update(config)
                # 确保每个数据源的配置完整，并解密API密钥
                for key in ["baostock", "tushare", "finnhub", "akshare"]:
                    if key in config:
                        result[key].update(config[key])
                        # 如果API密钥已加密，则解密
                        if result[key].get("api_key") and is_encrypted(result[key]["api_key"]):
                            result[key]["api_key"] = decrypt_api_key(result[key]["api_key"])
                return result
        except Exception as e:
            print(f"加载配置文件失败: {e}，使用默认配置")
            return DEFAULT_DATA_SOURCE_CONFIG
    else:
        # 如果文件不存在，创建默认配置文件
        save_data_source_config(DEFAULT_DATA_SOURCE_CONFIG)
        return DEFAULT_DATA_SOURCE_CONFIG

# 保存配置
def save_data_source_config(config):
    """保存数据源配置到文件（自动加密API密钥）"""
    try:
        # 创建配置副本，加密API密钥
        config_to_save = {}
        for key, value in config.items():
            if key in ["baostock", "tushare", "finnhub", "akshare"]:
                # 对数据源配置进行加密处理
                config_to_save[key] = {}
                config_to_save[key]["enabled"] = value.get("enabled", False)
                api_key = value.get("api_key")
                if api_key:
                    # 如果未加密，则加密；如果已加密，则保持原样
                    if not is_encrypted(api_key):
                        config_to_save[key]["api_key"] = encrypt_api_key(api_key)
                    else:
                        config_to_save[key]["api_key"] = api_key
                else:
                    config_to_save[key]["api_key"] = None
            else:
                # 其他配置项直接复制
                config_to_save[key] = value
        
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config_to_save, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"保存配置文件失败: {e}")

# 数据源配置（从文件加载）
DATA_SOURCE_CONFIG = load_data_source_config()

# 数据更新配置
UPDATE_CONFIG = {
    "auto_update": False,  # 是否自动更新
    "update_frequency": "monthly",  # 更新频率：monthly/weekly/daily
}

# Web服务配置
WEB_CONFIG = {
    "host": "0.0.0.0",
    "port": 8000,
    "debug": True,
}

# 统计配置
STATISTICS_CONFIG = {
    "min_total_count": 0,  # 最小总涨跌次数过滤（默认不过滤）
    "exclude_delisted": True,  # 排除退市股票
    "include_st_stocks": True,  # 包含ST股票
}


