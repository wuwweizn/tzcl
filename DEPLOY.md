# 部署说明文档

## 一、本地Windows使用

### 快速开始

1. **安装Python**
   - 下载并安装Python 3.8或更高版本
   - 下载地址: https://www.python.org/downloads/
   - 安装时勾选"Add Python to PATH"

2. **安装项目**
   - 双击运行 `install.bat`
   - 等待依赖包安装完成

3. **启动服务**
   - 双击运行 `start.bat`
   - 浏览器访问 http://localhost:8000

### 手动安装（可选）

如果自动安装失败，可以手动执行：

```cmd
# 安装依赖
pip install -r requirements.txt

# 初始化数据库
python init_db.py

# 启动服务
python main.py
```

---

## 二、Linux/VPS服务器部署

### 快速开始

1. **上传项目文件**
   ```bash
   # 使用scp或FTP工具上传项目文件夹到服务器
   scp -r /path/to/project user@your-server:/opt/stock-analysis
   ```

2. **SSH登录服务器**
   ```bash
   ssh user@your-server
   cd /opt/stock-analysis
   ```

3. **安装依赖**
   ```bash
   # 给安装脚本执行权限
   chmod +x install.sh start.sh
   
   # 运行安装脚本
   ./install.sh
   ```

4. **启动服务**
   ```bash
   # 直接启动（前台运行）
   ./start.sh
   
   # 或后台运行
   nohup ./start.sh > app.log 2>&1 &
   ```

### 使用systemd管理服务（推荐）

1. **创建systemd服务文件**
   ```bash
   sudo nano /etc/systemd/system/stock-analysis.service
   ```

2. **添加以下内容**
   ```ini
   [Unit]
   Description=股票月K统计分析系统
   After=network.target

   [Service]
   Type=simple
   User=your-username
   WorkingDirectory=/opt/stock-analysis
   ExecStart=/usr/bin/python3 /opt/stock-analysis/main.py
   Restart=always
   RestartSec=10

   [Install]
   WantedBy=multi-user.target
   ```

3. **启动和管理服务**
   ```bash
   # 重新加载systemd配置
   sudo systemctl daemon-reload
   
   # 启动服务
   sudo systemctl start stock-analysis
   
   # 设置开机自启
   sudo systemctl enable stock-analysis
   
   # 查看服务状态
   sudo systemctl status stock-analysis
   
   # 查看日志
   sudo journalctl -u stock-analysis -f
   ```

### 使用Nginx反向代理（可选）

1. **安装Nginx**
   ```bash
   sudo apt-get update
   sudo apt-get install nginx
   ```

2. **配置Nginx**
   ```bash
   sudo nano /etc/nginx/sites-available/stock-analysis
   ```

3. **添加以下配置**
   ```nginx
   server {
       listen 80;
       server_name your-domain.com;  # 替换为你的域名或IP

       location / {
           proxy_pass http://127.0.0.1:8000;
           proxy_set_header Host $host;
           proxy_set_header X-Real-IP $remote_addr;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
           proxy_set_header X-Forwarded-Proto $scheme;
       }
   }
   ```

4. **启用配置并重启Nginx**
   ```bash
   sudo ln -s /etc/nginx/sites-available/stock-analysis /etc/nginx/sites-enabled/
   sudo nginx -t
   sudo systemctl restart nginx
   ```

### 使用Docker部署（可选）

1. **创建Dockerfile**
   ```dockerfile
   FROM python:3.11-slim

   WORKDIR /app

   COPY requirements.txt .
   RUN pip install --no-cache-dir -r requirements.txt

   COPY . .

   EXPOSE 8000

   CMD ["python", "main.py"]
   ```

2. **构建和运行**
   ```bash
   docker build -t stock-analysis .
   docker run -d -p 8000:8000 --name stock-analysis stock-analysis
   ```

---

## 三、配置说明

### 数据源配置

1. **访问Web界面**
   - 打开浏览器访问 http://localhost:8000（本地）或 http://your-server-ip:8000（服务器）

2. **配置数据源**
   - 点击"配置管理"标签页
   - 启用需要的数据源（BaoStock/tushare/FinnHub/AKShare）
   - 输入API密钥（已配置的密钥会显示为掩码）
   - 点击"测试连接"验证配置
   - 点击"保存配置"保存设置

### 数据更新

1. **首次使用**
   - 点击"数据更新"标签页
   - 先点击"更新股票列表"获取所有股票
   - 然后点击"更新所有股票月K数据"（首次更新可能需要较长时间）

2. **定期更新**
   - 建议每月更新一次数据
   - 可以勾选"强制全量更新"来重新获取所有数据

---

## 四、防火墙配置

### Linux服务器

```bash
# Ubuntu/Debian
sudo ufw allow 8000/tcp

# CentOS/RHEL
sudo firewall-cmd --permanent --add-port=8000/tcp
sudo firewall-cmd --reload
```

### Windows服务器

在Windows防火墙中添加端口8000的入站规则。

---

## 五、常见问题

### 1. 端口被占用

如果8000端口被占用，可以修改 `config.py` 中的端口配置：

```python
WEB_CONFIG = {
    "host": "0.0.0.0",
    "port": 8001,  # 修改为其他端口
    "debug": False,
}
```

### 2. 数据库文件权限问题（Linux）

```bash
# 确保数据库文件有正确的权限
chmod 644 stock_analysis.db
chown your-username:your-username stock_analysis.db
```

### 3. 加密密钥文件权限（Linux）

```bash
# 确保加密密钥文件有正确的权限
chmod 600 .encryption_key
chown your-username:your-username .encryption_key
```

### 4. 内存不足

如果服务器内存较小，可以：
- 分批更新数据（不要一次性更新所有股票）
- 定期清理导出的Excel/CSV文件
- 增加服务器内存或使用swap

---

## 六、备份和恢复

### 备份

```bash
# 备份数据库
cp stock_analysis.db stock_analysis.db.backup

# 备份配置文件
cp data_source_config.json data_source_config.json.backup
cp .encryption_key .encryption_key.backup
```

### 恢复

```bash
# 恢复数据库
cp stock_analysis.db.backup stock_analysis.db

# 恢复配置文件
cp data_source_config.json.backup data_source_config.json
cp .encryption_key.backup .encryption_key
```

---

## 七、性能优化

1. **数据库优化**
   - 定期清理旧数据
   - 为常用查询字段添加索引

2. **服务器优化**
   - 使用SSD存储数据库
   - 增加服务器内存
   - 使用CDN加速静态资源（如需要）

---

## 八、安全建议

1. **生产环境**
   - 修改 `config.py` 中的 `debug = False`
   - 使用HTTPS（配置SSL证书）
   - 设置防火墙规则
   - 定期更新依赖包

2. **API密钥安全**
   - API密钥已加密存储
   - 不要将 `.encryption_key` 文件分享给他人
   - 定期更换API密钥

---

## 九、技术支持

如有问题，请检查：
1. Python版本是否符合要求（3.8+）
2. 依赖包是否完整安装
3. 数据库文件权限是否正确
4. 防火墙是否开放相应端口
5. 服务器日志文件（app.log）

---

## 十、更新说明

更新项目时：
1. 备份数据库和配置文件
2. 下载新版本文件
3. 运行 `pip install -r requirements.txt` 更新依赖
4. 重启服务

