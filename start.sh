#!/bin/bash

echo "========================================"
echo "  股票月K统计分析系统"
echo "========================================"
echo ""

# 检查Python是否安装
if ! command -v python3 &> /dev/null; then
    echo "[错误] 未检测到Python3，请先安装Python 3.8或更高版本"
    exit 1
fi

echo "[信息] 检测到Python环境"
python3 --version

# 检查依赖是否安装
echo ""
echo "[信息] 检查依赖包..."
if ! python3 -c "import fastapi" &> /dev/null; then
    echo "[警告] 检测到缺少依赖包，正在安装..."
    pip3 install -r requirements.txt
    if [ $? -ne 0 ]; then
        echo "[错误] 依赖包安装失败，请检查网络连接"
        exit 1
    fi
    echo "[成功] 依赖包安装完成"
else
    echo "[信息] 依赖包已安装"
fi

# 检查数据库是否初始化
if [ ! -f "stock_analysis.db" ]; then
    echo ""
    echo "[信息] 首次运行，正在初始化数据库..."
    python3 init_db.py
    if [ $? -ne 0 ]; then
        echo "[错误] 数据库初始化失败"
        exit 1
    fi
    echo "[成功] 数据库初始化完成"
fi

echo ""
echo "[信息] 启动Web服务..."
echo "[信息] 服务地址: http://localhost:8000"
echo "[信息] 按 Ctrl+C 停止服务"
echo ""
echo "========================================"
echo ""

python3 main.py

