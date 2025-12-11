@echo off
chcp 65001 >nul
echo ========================================
echo   股票月K统计分析系统
echo ========================================
echo.

REM 检查Python是否安装
python --version >nul 2>&1
if errorlevel 1 (
    echo [错误] 未检测到Python，请先安装Python 3.8或更高版本
    echo 下载地址: https://www.python.org/downloads/
    pause
    exit /b 1
)

echo [信息] 检测到Python环境
python --version

REM 检查依赖是否安装
echo.
echo [信息] 检查依赖包...
python -c "import fastapi" >nul 2>&1
if errorlevel 1 (
    echo [警告] 检测到缺少依赖包，正在安装...
    pip install -r requirements.txt
    if errorlevel 1 (
        echo [错误] 依赖包安装失败，请检查网络连接
        pause
        exit /b 1
    )
    echo [成功] 依赖包安装完成
) else (
    echo [信息] 依赖包已安装
)

REM 检查数据库是否初始化
if not exist "stock_analysis.db" (
    echo.
    echo [信息] 首次运行，正在初始化数据库...
    python init_db.py
    if errorlevel 1 (
        echo [错误] 数据库初始化失败
        pause
        exit /b 1
    )
    echo [成功] 数据库初始化完成
)

echo.
echo [信息] 启动Web服务...
echo [信息] 服务地址: http://localhost:8000
echo [信息] 按 Ctrl+C 停止服务
echo.
echo ========================================
echo.

python main.py

pause

