@echo off
chcp 65001 >nul
echo ========================================
echo   股票月K统计分析系统 - 安装程序
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
echo.

REM 安装依赖
echo [信息] 正在安装依赖包...
echo 这可能需要几分钟时间，请耐心等待...
echo.
pip install -r requirements.txt
if errorlevel 1 (
    echo [错误] 依赖包安装失败，请检查网络连接
    pause
    exit /b 1
)
echo.
echo [成功] 依赖包安装完成
echo.

REM 初始化数据库
echo [信息] 正在初始化数据库...
python init_db.py
if errorlevel 1 (
    echo [错误] 数据库初始化失败
    pause
    exit /b 1
)
echo [成功] 数据库初始化完成
echo.

echo ========================================
echo   安装完成！
echo ========================================
echo.
echo 使用说明：
echo 1. 运行 start.bat 启动服务
echo 2. 访问 http://localhost:8000
echo 3. 在"配置管理"中配置数据源API密钥
echo 4. 在"数据更新"中更新股票数据
echo.
pause

