// 全局变量
let currentQuery = null;

// 切换标签页
function switchTab(tabName) {
    // 隐藏所有标签内容
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    
    // 移除所有标签的active类
    document.querySelectorAll('.tab').forEach(tab => {
        tab.classList.remove('active');
    });
    
    // 显示选中的标签内容
    document.getElementById(tabName).classList.add('active');
    
    // 激活对应的标签按钮
    event.target.classList.add('active');
    
    // 如果是行业分析标签，加载行业列表
    if (tabName === 'industry' || tabName === 'batch') {
        loadIndustries();
    }
}

// 加载行业列表
async function loadIndustries() {
    try {
        const response = await fetch('/api/industries');
        const data = await response.json();
        
        const batchSelect = document.getElementById('batch-industry');
        const industrySelect = document.getElementById('industry-select');
        
        // 清空现有选项
        batchSelect.innerHTML = '<option value="">全部</option>';
        industrySelect.innerHTML = '<option value="">请选择行业</option>';
        
        // 添加行业选项
        data.industries.forEach(industry => {
            const option1 = document.createElement('option');
            option1.value = industry.code;
            option1.textContent = `${industry.name} (${industry.code})`;
            batchSelect.appendChild(option1);
            
            const option2 = document.createElement('option');
            option2.value = industry.code;
            option2.textContent = `${industry.name} (${industry.code})`;
            industrySelect.appendChild(option2);
        });
    } catch (error) {
        console.error('加载行业列表失败:', error);
    }
}

// 股票代码自动补全
let suggestionTimeout;
document.getElementById('single-stock-code').addEventListener('input', function(e) {
    const keyword = e.target.value.trim();
    const suggestionsDiv = document.getElementById('single-suggestions');
    
    clearTimeout(suggestionTimeout);
    
    if (keyword.length < 1) {
        suggestionsDiv.classList.remove('active');
        return;
    }
    
    suggestionTimeout = setTimeout(async () => {
        try {
            const response = await fetch(`/api/stocks/suggest?keyword=${encodeURIComponent(keyword)}`);
            const data = await response.json();
            
            suggestionsDiv.innerHTML = '';
            if (data.suggestions.length > 0) {
                data.suggestions.forEach(suggestion => {
                    const item = document.createElement('div');
                    item.className = 'suggestion-item';
                    item.textContent = `${suggestion.code} - ${suggestion.name}`;
                    item.onclick = () => {
                        document.getElementById('single-stock-code').value = suggestion.code;
                        suggestionsDiv.classList.remove('active');
                    };
                    suggestionsDiv.appendChild(item);
                });
                suggestionsDiv.classList.add('active');
            } else {
                suggestionsDiv.classList.remove('active');
            }
        } catch (error) {
            console.error('获取建议失败:', error);
        }
    }, 300);
});

// 点击外部关闭建议
document.addEventListener('click', function(e) {
    if (!e.target.closest('#single-stock-code') && !e.target.closest('#single-suggestions')) {
        document.getElementById('single-suggestions').classList.remove('active');
    }
});

// 获取选中的月份
function getSelectedMonths(selectorId) {
    const checkboxes = document.querySelectorAll(`#${selectorId} input[type="checkbox"]:checked`);
    if (checkboxes.length === 0) {
        return null;
    }
    return Array.from(checkboxes).map(cb => parseInt(cb.value));
}

// 查询单只股票
async function querySingleStock() {
    const stockCode = document.getElementById('single-stock-code').value.trim();
    if (!stockCode) {
        alert('请输入股票代码或名称');
        return;
    }
    
    const months = getSelectedMonths('single-month-selector');
    const minCount = parseInt(document.getElementById('single-min-count').value) || 0;
    
    // 显示/隐藏统计方式选择（多选月份时显示）
    const modeGroup = document.getElementById('single-statistics-mode-group');
    if (months && months.length > 1) {
        modeGroup.style.display = 'block';
    } else {
        modeGroup.style.display = 'none';
    }
    
    const statisticsMode = document.querySelector('input[name="single-statistics-mode"]:checked')?.value || 'summary';
    const groupByMonth = statisticsMode === 'monthly';
    
    const loadingDiv = document.getElementById('single-loading');
    const resultsDiv = document.getElementById('single-results');
    
    loadingDiv.classList.add('active');
    resultsDiv.innerHTML = '';
    
    try {
        const response = await fetch('/api/stocks/statistics', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                stock_code: stockCode,
                months: months,
                min_total_count: minCount,
                group_by_month: groupByMonth
            })
        });
        
        if (!response.ok) {
            throw new Error('查询失败');
        }
        
        const data = await response.json();
        displaySingleStockResult(data);
    } catch (error) {
        resultsDiv.innerHTML = `<div class="error">查询失败: ${error.message}</div>`;
    } finally {
        loadingDiv.classList.remove('active');
    }
}

// 显示单只股票结果
function displaySingleStockResult(data) {
    const resultsDiv = document.getElementById('single-results');
    
    if (!data) {
        resultsDiv.innerHTML = '<div class="error">未找到统计数据</div>';
        return;
    }
    
    let html = '';
    
    if (data.statistics_mode === 'monthly' && data.monthly_statistics) {
        // 按月统计模式
        html = `<div class="stats-card">
            <h3>${data.stock_name} (${data.stock_code}) - 按月统计</h3>`;
        
        const monthNames = ['', '1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月'];
        
        // 显示各月份统计
        for (const [month, stats] of Object.entries(data.monthly_statistics).sort((a, b) => parseInt(a) - parseInt(b))) {
            html += `
            <div style="margin-top: 20px; padding: 15px; background: #f8f9fa; border-radius: 5px;">
                <h4 style="margin-top: 0; color: #667eea;">${monthNames[parseInt(month)]}统计</h4>
                <div class="stats-grid">
                    <div class="stat-item">
                        <div class="stat-label">上涨次数</div>
                        <div class="stat-value">${stats.up_count}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">下跌次数</div>
                        <div class="stat-value">${stats.down_count}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">上涨概率</div>
                        <div class="stat-value">${stats.up_probability}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">下跌概率</div>
                        <div class="stat-value">${stats.down_probability}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">平均涨幅</div>
                        <div class="stat-value">${stats.avg_up_pct}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">平均跌幅</div>
                        <div class="stat-value">${stats.avg_down_pct}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">总涨跌次数</div>
                        <div class="stat-value">${stats.total_count}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">统计年份范围</div>
                        <div class="stat-value">${stats.year_range}</div>
                    </div>
                </div>
            </div>`;
        }
        
        // 显示汇总统计作为参考
        if (data.summary_statistics) {
            const summary = data.summary_statistics;
            html += `
            <div style="margin-top: 20px; padding: 15px; background: #e7f3ff; border-radius: 5px; border-left: 4px solid #667eea;">
                <h4 style="margin-top: 0; color: #667eea;">汇总统计（所有选中月份合并）</h4>
                <div class="stats-grid">
                    <div class="stat-item">
                        <div class="stat-label">上涨次数</div>
                        <div class="stat-value">${summary.up_count}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">下跌次数</div>
                        <div class="stat-value">${summary.down_count}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">上涨概率</div>
                        <div class="stat-value">${summary.up_probability}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">下跌概率</div>
                        <div class="stat-value">${summary.down_probability}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">平均涨幅</div>
                        <div class="stat-value">${summary.avg_up_pct}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">平均跌幅</div>
                        <div class="stat-value">${summary.avg_down_pct}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">总涨跌次数</div>
                        <div class="stat-value">${summary.total_count}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">统计年份范围</div>
                        <div class="stat-value">${summary.year_range}</div>
                    </div>
                </div>
            </div>`;
        }
        
        html += '</div>';
    } else {
        // 汇总统计模式
        html = `
        <div class="stats-card">
            <h3>${data.stock_name} (${data.stock_code})</h3>
            <div class="stats-grid">
                <div class="stat-item">
                    <div class="stat-label">上涨次数</div>
                    <div class="stat-value">${data.up_count}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">下跌次数</div>
                    <div class="stat-value">${data.down_count}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">上涨概率</div>
                    <div class="stat-value">${data.up_probability}%</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">下跌概率</div>
                    <div class="stat-value">${data.down_probability}%</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">平均涨幅</div>
                    <div class="stat-value">${data.avg_up_pct}%</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">平均跌幅</div>
                    <div class="stat-value">${data.avg_down_pct}%</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">统计年份范围</div>
                    <div class="stat-value">${data.year_range}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">总涨跌次数</div>
                    <div class="stat-value">${data.total_count}</div>
                </div>
            </div>
        </div>
    `;
    }
    
    resultsDiv.innerHTML = html;
}

// 批量查询
async function queryBatch() {
    const months = getSelectedMonths('batch-month-selector');
    const market = document.getElementById('batch-market').value;
    const industry = document.getElementById('batch-industry').value;
    const minCount = parseInt(document.getElementById('batch-min-count').value) || 0;
    const limit = parseInt(document.getElementById('batch-limit').value) || 20;
    const orderBy = document.getElementById('batch-order-by').value;
    
    currentQuery = {
        months: months,
        market: market || null,
        industry_code: industry || null,
        min_total_count: minCount,
        limit: limit,
        order_by: orderBy
    };
    
    const loadingDiv = document.getElementById('batch-loading');
    const resultsDiv = document.getElementById('batch-results');
    
    loadingDiv.classList.add('active');
    resultsDiv.innerHTML = '';
    
    try {
        const response = await fetch('/api/stocks/batch', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(currentQuery)
        });
        
        if (!response.ok) {
            throw new Error('查询失败');
        }
        
        const data = await response.json();
        displayBatchResults(data.results);
    } catch (error) {
        resultsDiv.innerHTML = `<div class="error">查询失败: ${error.message}</div>`;
    } finally {
        loadingDiv.classList.remove('active');
    }
}

// 显示批量查询结果
function displayBatchResults(results) {
    const resultsDiv = document.getElementById('batch-results');
    
    if (results.length === 0) {
        resultsDiv.innerHTML = '<div class="error">未找到符合条件的股票</div>';
        return;
    }
    
    let tableHTML = `
        <table class="results-table">
            <thead>
                <tr>
                    <th>排名</th>
                    <th>股票代码</th>
                    <th>股票名称</th>
                    <th>上涨次数</th>
                    <th>下跌次数</th>
                    <th>上涨概率</th>
                    <th>平均涨幅</th>
                    <th>平均跌幅</th>
                    <th>统计年份范围</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    results.forEach(stock => {
        tableHTML += `
            <tr>
                <td>${stock.rank}</td>
                <td>${stock.stock_code}</td>
                <td>${stock.stock_name}</td>
                <td>${stock.up_count}</td>
                <td>${stock.down_count}</td>
                <td>${stock.up_probability}%</td>
                <td>${stock.avg_up_pct}%</td>
                <td>${stock.avg_down_pct}%</td>
                <td>${stock.year_range}</td>
            </tr>
        `;
    });
    
    tableHTML += `
            </tbody>
        </table>
    `;
    
    resultsDiv.innerHTML = tableHTML;
    
    // 绘制图表
    drawBatchChart(results);
}

// 绘制批量查询图表
function drawBatchChart(results) {
    const chartDiv = document.createElement('div');
    chartDiv.className = 'chart-container';
    chartDiv.id = 'batch-chart';
    document.getElementById('batch-results').appendChild(chartDiv);
    
    const top20 = results.slice(0, 20);
    const codes = top20.map(s => s.stock_code);
    const probabilities = top20.map(s => s.up_probability);
    
    const trace = {
        x: codes,
        y: probabilities,
        type: 'bar',
        marker: {
            color: probabilities,
            colorscale: 'Viridis'
        }
    };
    
    const layout = {
        title: '前20支股票上涨概率',
        xaxis: { title: '股票代码' },
        yaxis: { title: '上涨概率 (%)' }
    };
    
    Plotly.newPlot('batch-chart', [trace], layout);
}

// 查询行业
async function queryIndustry() {
    const industryCode = document.getElementById('industry-select').value;
    if (!industryCode) {
        alert('请选择行业');
        return;
    }
    
    const months = getSelectedMonths('industry-month-selector');
    const minCount = parseInt(document.getElementById('industry-min-count')?.value || '0') || 0;
    
    // 获取统计方式
    const statisticsModeGroup = document.getElementById('industry-statistics-mode-group');
    let groupByMonth = false;
    
    // 如果选择了多个月份，需要获取统计方式
    if (months && months.length > 1) {
        // 无论统计方式选择是否显示，都尝试获取选中的值
        const mode = document.querySelector('input[name="industry-statistics-mode"]:checked');
        if (mode) {
            groupByMonth = mode.value === 'monthly';
            console.log('获取统计方式:', mode.value, 'groupByMonth:', groupByMonth);
        } else {
            // 如果没有找到选中的，默认使用按月统计（因为HTML中默认checked的是monthly）
            groupByMonth = true;
            console.log('未找到选中的统计方式，使用默认值: monthly');
        }
    }
    
    console.log('发送参数:', { industry_code: industryCode, months: months, min_total_count: minCount, group_by_month: groupByMonth });
    
    const loadingDiv = document.getElementById('industry-loading');
    const resultsDiv = document.getElementById('industry-results');
    
    loadingDiv.classList.add('active');
    resultsDiv.innerHTML = '';
    
    try {
        const response = await fetch('/api/industries/statistics', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                industry_code: industryCode,
                months: months,
                min_total_count: minCount,
                group_by_month: groupByMonth
            })
        });
        
        if (!response.ok) {
            throw new Error('查询失败');
        }
        
        const data = await response.json();
        console.log('行业分析返回数据:', data);
        console.log('statistics_mode:', data.statistics_mode);
        console.log('has monthly_statistics:', 'monthly_statistics' in data);
        displayIndustryResult(data);
    } catch (error) {
        resultsDiv.innerHTML = `<div class="error">查询失败: ${error.message}</div>`;
    } finally {
        loadingDiv.classList.remove('active');
    }
}

// 显示行业结果
function displayIndustryResult(data) {
    const resultsDiv = document.getElementById('industry-results');
    let html = '';
    
    console.log('displayIndustryResult - statistics_mode:', data.statistics_mode);
    console.log('displayIndustryResult - monthly_statistics:', data.monthly_statistics);
    console.log('displayIndustryResult - condition check:', data.statistics_mode === 'monthly' && data.monthly_statistics);
    
    if (data.statistics_mode === 'monthly' && data.monthly_statistics) {
        // 按月统计模式
        html = `<div class="stats-card">
            <h3>${data.industry_name} - 按月统计</h3>
            <p style="color: #666; margin-bottom: 20px;">股票数量: ${data.stock_count}</p>`;
        
        const monthNames = ['', '1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月'];
        
        for (const [month, stats] of Object.entries(data.monthly_statistics).sort((a, b) => parseInt(a[0]) - parseInt(b[0]))) {
            html += `
            <div style="margin-bottom: 20px; padding: 15px; background: #f8f9fa; border-radius: 5px; border-left: 4px solid #667eea;">
                <h4 style="margin-top: 0; color: #667eea;">${monthNames[parseInt(month)]}</h4>
                <div class="stats-grid">
                    <div class="stat-item">
                        <div class="stat-label">上涨次数</div>
                        <div class="stat-value">${stats.up_count}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">下跌次数</div>
                        <div class="stat-value">${stats.down_count}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">上涨概率</div>
                        <div class="stat-value">${stats.up_probability}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">下跌概率</div>
                        <div class="stat-value">${stats.down_probability}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">平均涨幅</div>
                        <div class="stat-value">${stats.avg_up_pct}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">平均跌幅</div>
                        <div class="stat-value">${stats.avg_down_pct}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">总涨跌次数</div>
                        <div class="stat-value">${stats.total_count}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">统计年份范围</div>
                        <div class="stat-value">${stats.year_range}</div>
                    </div>
                </div>
            </div>`;
        }
        
        // 显示汇总统计作为参考
        if (data.summary_statistics) {
            const summary = data.summary_statistics;
            html += `
            <div style="margin-top: 20px; padding: 15px; background: #e7f3ff; border-radius: 5px; border-left: 4px solid #667eea;">
                <h4 style="margin-top: 0; color: #667eea;">汇总统计（所有选中月份合并）</h4>
                <div class="stats-grid">
                    <div class="stat-item">
                        <div class="stat-label">上涨次数</div>
                        <div class="stat-value">${summary.up_count}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">下跌次数</div>
                        <div class="stat-value">${summary.down_count}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">上涨概率</div>
                        <div class="stat-value">${summary.up_probability}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">下跌概率</div>
                        <div class="stat-value">${summary.down_probability}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">平均涨幅</div>
                        <div class="stat-value">${summary.avg_up_pct}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">平均跌幅</div>
                        <div class="stat-value">${summary.avg_down_pct}%</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">总涨跌次数</div>
                        <div class="stat-value">${summary.total_count}</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-label">统计年份范围</div>
                        <div class="stat-value">${summary.year_range}</div>
                    </div>
                </div>
            </div>`;
        }
        
        html += '</div>';
    } else {
        // 汇总统计模式
        html = `
        <div class="stats-card">
            <h3>${data.industry_name}</h3>
            <div class="stats-grid">
                <div class="stat-item">
                    <div class="stat-label">股票数量</div>
                    <div class="stat-value">${data.stock_count}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">上涨次数</div>
                    <div class="stat-value">${data.up_count}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">下跌次数</div>
                    <div class="stat-value">${data.down_count}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">上涨概率</div>
                    <div class="stat-value">${data.up_probability}%</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">下跌概率</div>
                    <div class="stat-value">${data.down_probability}%</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">平均涨幅</div>
                    <div class="stat-value">${data.avg_up_pct}%</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">平均跌幅</div>
                    <div class="stat-value">${data.avg_down_pct}%</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">总涨跌次数</div>
                    <div class="stat-value">${data.total_count}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">统计年份范围</div>
                    <div class="stat-value">${data.year_range}</div>
                </div>
            </div>
        </div>
    `;
    }
    
    resultsDiv.innerHTML = html;
}

// 查询行业排名（使用SSE流式返回）
async function queryIndustryRank() {
    const month = parseInt(document.getElementById('rank-month-select').value);
    const minCount = parseInt(document.getElementById('rank-min-count')?.value || '0') || 0;
    const limit = parseInt(document.getElementById('rank-limit')?.value || '20') || 20;
    
    if (!month || month < 1 || month > 12) {
        alert('请选择有效的月份');
        return;
    }
    
    const loadingDiv = document.getElementById('industry-rank-loading');
    const resultsDiv = document.getElementById('industry-rank-results');
    
    loadingDiv.classList.add('active');
    resultsDiv.innerHTML = '<div style="text-align: center; padding: 20px; color: #666;">正在查询所有行业数据，请稍候...</div>';
    
    try {
        const response = await fetch('/api/industries/rank-by-month', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'text/event-stream'
            },
            body: JSON.stringify({
                month: month,
                min_total_count: minCount,
                limit: limit
            })
        });
        
        if (!response.ok) {
            throw new Error(`请求失败: ${response.status} ${response.statusText}`);
        }
        
        if (!response.body) {
            throw new Error('响应体为空');
        }
        
        // 读取流式响应
        const reader = response.body.getReader();
        const decoder = new TextDecoder('utf-8');
        let buffer = '';
        let finalResult = null;
        
        while (true) {
            const { done, value } = await reader.read();
            
            if (done) {
                // 处理缓冲区中剩余的数据
                if (buffer.trim()) {
                    const lines = buffer.split('\n');
                    for (const line of lines) {
                        if (line.trim() && line.startsWith('data: ')) {
                            try {
                                const data = JSON.parse(line.slice(6));
                                if (data.done) {
                                    finalResult = data;
                                } else if (data.error) {
                                    throw new Error(data.message || '查询失败');
                                } else {
                                    // 更新进度
                                    updateIndustryRankProgress(data);
                                }
                            } catch (e) {
                                console.error('解析数据失败:', e);
                            }
                        }
                    }
                }
                break;
            }
            
            // 解码数据并添加到缓冲区
            buffer += decoder.decode(value, { stream: true });
            
            // 处理完整的行
            const lines = buffer.split('\n');
            buffer = lines.pop() || ''; // 保留最后一行（可能不完整）
            
            for (const line of lines) {
                if (line.trim() && line.startsWith('data: ')) {
                    try {
                        const data = JSON.parse(line.slice(6));
                        if (data.done) {
                            finalResult = data;
                        } else if (data.error) {
                            throw new Error(data.message || '查询失败');
                        } else {
                            // 更新进度
                            updateIndustryRankProgress(data);
                        }
                    } catch (e) {
                        console.error('解析进度数据失败:', e);
                    }
                }
            }
        }
        
        // 显示最终结果
        if (finalResult && finalResult.results) {
            displayIndustryRankResult(finalResult);
        } else if (finalResult && finalResult.error) {
            resultsDiv.innerHTML = `<div class="error">查询失败: ${finalResult.message || '未知错误'}</div>`;
        } else {
            resultsDiv.innerHTML = '<div class="error">未收到查询结果</div>';
        }
    } catch (error) {
        resultsDiv.innerHTML = `<div class="error">查询失败: ${error.message}</div>`;
        console.error('查询行业排名失败:', error);
    } finally {
        loadingDiv.classList.remove('active');
    }
}

// 更新行业排名查询进度
function updateIndustryRankProgress(data) {
    const resultsDiv = document.getElementById('industry-rank-results');
    const percent = data.percent || 0;
    const message = data.message || '正在查询...';
    const current = data.current || 0;
    const total = data.total || 0;
    
    resultsDiv.innerHTML = `
        <div style="text-align: center; padding: 20px;">
            <div style="color: #666; margin-bottom: 10px;">${message}</div>
            <div style="width: 100%; background: #f0f0f0; border-radius: 5px; overflow: hidden;">
                <div style="width: ${percent}%; background: #667eea; height: 30px; transition: width 0.3s; display: flex; align-items: center; justify-content: center; color: white; font-weight: bold;">
                    ${percent}%
                </div>
            </div>
            <div style="color: #999; margin-top: 10px; font-size: 14px;">${current}/${total}</div>
        </div>
    `;
}

// 显示行业排名结果
function displayIndustryRankResult(data) {
    const resultsDiv = document.getElementById('industry-rank-results');
    
    if (!data.results || data.results.length === 0) {
        resultsDiv.innerHTML = '<div class="error">未找到符合条件的行业数据</div>';
        return;
    }
    
    const monthNames = ['', '1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月', '12月'];
    
    // 显示统计信息
    const totalIndustries = data.total_industries || data.count;
    const successCount = data.success_count || data.count;
    const failedCount = data.failed_count || 0;
    const statsInfo = totalIndustries > data.count ? 
        `共查询 ${totalIndustries} 个行业，成功 ${successCount} 个，失败 ${failedCount} 个，显示前 ${data.count} 个` :
        `共找到 ${data.count} 个行业`;
    
    let html = `
        <div class="stats-card">
            <h3>${monthNames[data.month]} 行业上涨概率排名</h3>
            <p style="color: #666; margin-bottom: 20px;">${statsInfo}</p>
            <table class="data-table" style="width: 100%; border-collapse: collapse;">
                <thead>
                    <tr style="background: #f8f9fa; border-bottom: 2px solid #dee2e6;">
                        <th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">排名</th>
                        <th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">行业名称</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #dee2e6;">上涨次数</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #dee2e6;">下跌次数</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #dee2e6;">总次数</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #dee2e6;">上涨概率</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #dee2e6;">下跌概率</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #dee2e6;">平均涨幅</th>
                        <th style="padding: 12px; text-align: right; border-bottom: 2px solid #dee2e6;">平均跌幅</th>
                        <th style="padding: 12px; text-align: left; border-bottom: 2px solid #dee2e6;">年份范围</th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    data.results.forEach((item, index) => {
        const rowClass = index % 2 === 0 ? '' : 'style="background: #f8f9fa;"';
        html += `
                    <tr ${rowClass}>
                        <td style="padding: 10px; font-weight: bold; color: #667eea;">${index + 1}</td>
                        <td style="padding: 10px; font-weight: 500;">${item.industry_name}</td>
                        <td style="padding: 10px; text-align: right; color: #28a745;">${item.up_count}</td>
                        <td style="padding: 10px; text-align: right; color: #dc3545;">${item.down_count}</td>
                        <td style="padding: 10px; text-align: right;">${item.total_count}</td>
                        <td style="padding: 10px; text-align: right; font-weight: bold; color: #28a745;">${item.up_probability}%</td>
                        <td style="padding: 10px; text-align: right; color: #dc3545;">${item.down_probability}%</td>
                        <td style="padding: 10px; text-align: right; color: #28a745;">${item.avg_up_pct}%</td>
                        <td style="padding: 10px; text-align: right; color: #dc3545;">${item.avg_down_pct}%</td>
                        <td style="padding: 10px;">${item.year_range}</td>
                    </tr>
        `;
    });
    
    html += `
                </tbody>
            </table>
        </div>
    `;
    
    resultsDiv.innerHTML = html;
}

// 保存配置
async function saveConfig() {
    // 获取输入值
    const tushareInput = document.getElementById('config-tushare-token').value.trim();
    const finnhubInput = document.getElementById('config-finnhub-key').value.trim();
    
    // 如果输入值是掩码格式（包含*），说明用户没有修改，不发送API密钥
    // 如果输入值不是掩码格式，说明用户输入了新值，需要发送
    const tushareToken = (tushareInput && !isMaskedValue(tushareInput)) ? tushareInput : null;
    const finnhubKey = (finnhubInput && !isMaskedValue(finnhubInput)) ? finnhubInput : null;
    
    const config = {
        baostock_enabled: document.getElementById('config-baostock').checked,
        tushare_enabled: document.getElementById('config-tushare').checked,
        tushare_token: tushareToken,
        finnhub_enabled: document.getElementById('config-finnhub').checked,
        finnhub_api_key: finnhubKey
    };
    
    try {
        const response = await fetch('/api/config', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(config)
        });
        
        if (!response.ok) {
            throw new Error('保存失败');
        }
        
        document.getElementById('config-message').innerHTML = 
            '<div class="success">配置保存成功</div>';
        
        // 保存成功后重新加载配置（更新掩码值）
        loadConfig();
    } catch (error) {
        document.getElementById('config-message').innerHTML = 
            `<div class="error">保存失败: ${error.message}</div>`;
    }
}

// 测试数据源连接
async function testConnections() {
    // 获取输入值
    const tushareInput = document.getElementById('config-tushare-token').value.trim();
    const finnhubInput = document.getElementById('config-finnhub-key').value.trim();
    
    // 如果输入值是掩码格式，说明用户没有修改，不发送API密钥
    // 如果输入值不是掩码格式，说明用户输入了新值，需要发送
    const tushareToken = (tushareInput && !isMaskedValue(tushareInput)) ? tushareInput : null;
    const finnhubKey = (finnhubInput && !isMaskedValue(finnhubInput)) ? finnhubInput : null;
    
    const config = {
        baostock_enabled: document.getElementById('config-baostock').checked,
        tushare_enabled: document.getElementById('config-tushare').checked,
        tushare_token: tushareToken,
        finnhub_enabled: document.getElementById('config-finnhub').checked,
        finnhub_api_key: finnhubKey
    };
    
    // 清空之前的结果
    document.getElementById('test-baostock-result').innerHTML = '<span style="color: #666;">测试中...</span>';
    document.getElementById('test-tushare-result').innerHTML = '<span style="color: #666;">测试中...</span>';
    document.getElementById('test-finnhub-result').innerHTML = '<span style="color: #666;">测试中...</span>';
    document.getElementById('test-akshare-result').innerHTML = '<span style="color: #666;">测试中...</span>';
    document.getElementById('config-message').innerHTML = '';
    
    try {
        const response = await fetch('/api/config/test-connection', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(config)
        });
        
        if (!response.ok) {
            throw new Error('测试失败');
        }
        
        const results = await response.json();
        
        // 显示BaoStock测试结果
        const baostockResult = results.baostock;
        const baostockDiv = document.getElementById('test-baostock-result');
        if (baostockResult.success) {
            baostockDiv.innerHTML = `<span style="color: #28a745;">✅ ${baostockResult.message}</span>`;
        } else {
            baostockDiv.innerHTML = `<span style="color: #dc3545;">❌ ${baostockResult.message}</span>`;
        }
        
        // 显示tushare测试结果
        const tushareResult = results.tushare;
        const tushareDiv = document.getElementById('test-tushare-result');
        if (tushareResult.success) {
            tushareDiv.innerHTML = `<span style="color: #28a745;">✅ ${tushareResult.message}</span>`;
        } else {
            tushareDiv.innerHTML = `<span style="color: #dc3545;">❌ ${tushareResult.message}</span>`;
        }
        
        // 显示FinnHub测试结果
        const finnhubResult = results.finnhub;
        const finnhubDiv = document.getElementById('test-finnhub-result');
        if (finnhubResult.success) {
            finnhubDiv.innerHTML = `<span style="color: #28a745;">✅ ${finnhubResult.message}</span>`;
        } else {
            finnhubDiv.innerHTML = `<span style="color: #dc3545;">❌ ${finnhubResult.message}</span>`;
        }
        
        // 显示AKShare测试结果
        const akshareResult = results.akshare;
        const akshareDiv = document.getElementById('test-akshare-result');
        if (akshareResult.success) {
            akshareDiv.innerHTML = `<span style="color: #28a745;">✅ ${akshareResult.message}</span>`;
        } else {
            akshareDiv.innerHTML = `<span style="color: #dc3545;">❌ ${akshareResult.message}</span>`;
        }
        
        // 显示总体结果
        const allSuccess = baostockResult.success || tushareResult.success || finnhubResult.success || akshareResult.success;
        if (allSuccess) {
            document.getElementById('config-message').innerHTML = '<div class="success">连接测试完成，至少有一个数据源可用</div>';
        } else {
            document.getElementById('config-message').innerHTML = '<div class="error">连接测试完成，但没有可用的数据源</div>';
        }
    } catch (error) {
        document.getElementById('config-message').innerHTML = `<div class="error">测试失败: ${error.message}</div>`;
        document.getElementById('test-baostock-result').innerHTML = '<span style="color: #dc3545;">测试失败</span>';
        document.getElementById('test-tushare-result').innerHTML = '<span style="color: #dc3545;">测试失败</span>';
        document.getElementById('test-finnhub-result').innerHTML = '<span style="color: #dc3545;">测试失败</span>';
        document.getElementById('test-akshare-result').innerHTML = '<span style="color: #dc3545;">测试失败</span>';
    }
}

// 更新股票列表（带进度显示）
async function updateStockList() {
    const messageDiv = document.getElementById('update-message');
    const button = event?.target || document.querySelector('button[onclick="updateStockList()"]');
    
    // 禁用按钮，防止重复点击
    if (button) {
        button.disabled = true;
        button.textContent = '更新中...';
    }
    
    messageDiv.innerHTML = `
        <div>
            <div style="margin-bottom: 10px; font-weight: bold;">正在更新股票列表，请稍候...</div>
            <div class="progress-container">
                <div class="progress-bar" id="stock-list-progress-bar" style="width: 0%;">0%</div>
            </div>
            <div id="stock-list-progress-text" style="margin-top: 10px; color: #666; font-size: 14px; min-height: 20px;">准备中...</div>
        </div>
    `;
    
    const progressBar = document.getElementById('stock-list-progress-bar');
    const progressText = document.getElementById('stock-list-progress-text');
    
    try {
        const response = await fetch('/api/data/update-stock-list', {
            method: 'POST',
            headers: {
                'Accept': 'text/event-stream'
            }
        });
        
        if (!response.ok) {
            throw new Error(`请求失败: ${response.status} ${response.statusText}`);
        }
        
        if (!response.body) {
            throw new Error('响应体为空');
        }
        
        // 读取流式响应
        const reader = response.body.getReader();
        const decoder = new TextDecoder('utf-8');
        let buffer = '';
        
        while (true) {
            const { done, value } = await reader.read();
            
            if (done) {
                // 处理缓冲区中剩余的数据
                if (buffer.trim()) {
                    const lines = buffer.split('\n');
                    for (const line of lines) {
                        if (line.trim() && line.startsWith('data: ')) {
                            try {
                                const data = JSON.parse(line.slice(6));
                                updateProgress(data);
                            } catch (e) {
                                console.error('解析最终数据失败:', e);
                            }
                        }
                    }
                }
                break;
            }
            
            // 解码数据并添加到缓冲区
            buffer += decoder.decode(value, { stream: true });
            
            // 处理完整的行
            const lines = buffer.split('\n');
            buffer = lines.pop() || ''; // 保留最后一行（可能不完整）
            
            for (const line of lines) {
                const trimmedLine = line.trim();
                if (trimmedLine && trimmedLine.startsWith('data: ')) {
                    try {
                        const jsonStr = trimmedLine.slice(6);
                        const data = JSON.parse(jsonStr);
                        updateProgress(data);
                    } catch (e) {
                        console.error('解析进度数据失败:', e, '原始数据:', trimmedLine);
                    }
                }
            }
        }
    } catch (error) {
        console.error('更新失败:', error);
        messageDiv.innerHTML = `<div class="error">更新失败: ${error.message}<br>请查看浏览器控制台或 stock_analysis.log 文件获取详细错误信息</div>`;
    } finally {
        // 恢复按钮
        if (button) {
            button.disabled = false;
            button.textContent = '更新股票列表';
        }
    }
    
    function updateProgress(data) {
        // 更新进度条
        const percent = Math.max(0, Math.min(100, data.percent || 0));
        if (progressBar) {
            progressBar.style.width = percent + '%';
            progressBar.textContent = percent + '%';
        }
        
        // 更新进度文本
        if (data.message && progressText) {
            progressText.textContent = data.message;
        }
        
        // 如果完成或出错，显示最终消息
        if (data.done || data.error) {
            setTimeout(() => {
                if (data.error) {
                    messageDiv.innerHTML = `<div class="error">${data.message || '更新失败'}</div>`;
                } else {
                    messageDiv.innerHTML = `<div class="success">${data.message || '更新完成'}</div>`;
                }
            }, 500);
        }
    }
}

// 更新月K数据（带进度显示）
async function updateMonthlyData() {
    const messageDiv = document.getElementById('update-message');
    const forceUpdate = document.getElementById('force-update').checked;
    const button = event?.target || document.querySelector('button[onclick="updateMonthlyData()"]');
    
    // 禁用按钮，防止重复点击
    if (button) {
        button.disabled = true;
        const originalText = button.textContent;
        button.textContent = '更新中...';
    }
    
    messageDiv.innerHTML = `
        <div>
            <div style="margin-bottom: 10px; font-weight: bold;">正在更新月K数据，这可能需要较长时间，请耐心等待...</div>
            <div class="progress-container">
                <div class="progress-bar" id="monthly-progress-bar" style="width: 0%;">0%</div>
            </div>
            <div id="monthly-progress-text" style="margin-top: 10px; color: #666; font-size: 14px; min-height: 20px;">准备中...</div>
        </div>
    `;
    
    const progressBar = document.getElementById('monthly-progress-bar');
    const progressText = document.getElementById('monthly-progress-text');
    
    try {
        const response = await fetch('/api/data/update', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'text/event-stream'
            },
            body: JSON.stringify({
                stock_codes: null,
                force_update: forceUpdate
            })
        });
        
        if (!response.ok) {
            throw new Error(`请求失败: ${response.status} ${response.statusText}`);
        }
        
        if (!response.body) {
            throw new Error('响应体为空');
        }
        
        // 读取流式响应
        const reader = response.body.getReader();
        const decoder = new TextDecoder('utf-8');
        let buffer = '';
        
        while (true) {
            const { done, value } = await reader.read();
            
            if (done) {
                // 处理缓冲区中剩余的数据
                if (buffer.trim()) {
                    const lines = buffer.split('\n');
                    for (const line of lines) {
                        if (line.trim() && line.startsWith('data: ')) {
                            try {
                                const data = JSON.parse(line.slice(6));
                                updateProgress(data);
                            } catch (e) {
                                console.error('解析最终数据失败:', e);
                            }
                        }
                    }
                }
                break;
            }
            
            // 解码数据并添加到缓冲区
            buffer += decoder.decode(value, { stream: true });
            
            // 处理完整的行
            const lines = buffer.split('\n');
            buffer = lines.pop() || ''; // 保留最后一行（可能不完整）
            
            for (const line of lines) {
                const trimmedLine = line.trim();
                if (trimmedLine && trimmedLine.startsWith('data: ')) {
                    try {
                        const jsonStr = trimmedLine.slice(6);
                        const data = JSON.parse(jsonStr);
                        updateProgress(data);
                    } catch (e) {
                        console.error('解析进度数据失败:', e, '原始数据:', trimmedLine);
                    }
                }
            }
        }
    } catch (error) {
        console.error('更新失败:', error);
        messageDiv.innerHTML = `<div class="error">更新失败: ${error.message}<br>请查看浏览器控制台或 stock_analysis.log 文件获取详细错误信息</div>`;
    } finally {
        // 恢复按钮
        if (button) {
            button.disabled = false;
            button.textContent = '更新所有股票月K数据';
        }
    }
    
    function updateProgress(data) {
        // 更新进度条
        const percent = Math.max(0, Math.min(100, data.percent || 0));
        if (progressBar) {
            progressBar.style.width = percent + '%';
            progressBar.textContent = percent + '%';
        }
        
        // 更新进度文本
        if (data.message && progressText) {
            progressText.textContent = data.message;
        }
        
        // 如果完成或出错，显示最终消息
        if (data.done || data.error) {
            setTimeout(() => {
                if (data.error) {
                    messageDiv.innerHTML = `<div class="error">${data.message || '更新失败'}</div>`;
                } else {
                    messageDiv.innerHTML = `<div class="success">${data.message || '更新完成'}</div>`;
                }
            }, 500);
        }
    }
}

// 导出Excel
async function exportExcel() {
    if (!currentQuery) {
        alert('请先执行查询');
        return;
    }
    
    try {
        const response = await fetch('/api/export/excel', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(currentQuery)
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: '导出失败' }));
            throw new Error(errorData.detail || '导出失败');
        }
        
        // 从响应头获取文件名
        const contentDisposition = response.headers.get('Content-Disposition');
        let filename = `stock_statistics_${new Date().toISOString().slice(0,10)}.xlsx`;
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename="?(.+)"?/);
            if (filenameMatch) {
                filename = filenameMatch[1];
            }
        }
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
    } catch (error) {
        alert(`导出失败: ${error.message}`);
    }
}

// 导出CSV
async function exportCSV() {
    if (!currentQuery) {
        alert('请先执行查询');
        return;
    }
    
    try {
        const response = await fetch('/api/export/csv', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(currentQuery)
        });
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({ detail: '导出失败' }));
            throw new Error(errorData.detail || '导出失败');
        }
        
        // 从响应头获取文件名
        const contentDisposition = response.headers.get('Content-Disposition');
        let filename = `stock_statistics_${new Date().toISOString().slice(0,10)}.csv`;
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename="?(.+)"?/);
            if (filenameMatch) {
                filename = filenameMatch[1];
            }
        }
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
    } catch (error) {
        alert(`导出失败: ${error.message}`);
    }
}

// 页面加载时初始化
// 监听单只股票月份选择变化，动态显示/隐藏统计方式选择
function setupMonthSelectorListener() {
    const monthCheckboxes = document.querySelectorAll('#single-month-selector input[type="checkbox"]');
    const modeGroup = document.getElementById('single-statistics-mode-group');
    
    monthCheckboxes.forEach(checkbox => {
        checkbox.addEventListener('change', function() {
            const selectedMonths = getSelectedMonths('single-month-selector');
            if (selectedMonths && selectedMonths.length > 1) {
                modeGroup.style.display = 'block';
            } else {
                modeGroup.style.display = 'none';
            }
        });
    });
}

// 监听行业分析月份选择变化，动态显示/隐藏统计方式选择
function setupIndustryMonthSelectorListener() {
    const monthCheckboxes = document.querySelectorAll('#industry-month-selector input[type="checkbox"]');
    const modeGroup = document.getElementById('industry-statistics-mode-group');
    
    monthCheckboxes.forEach(checkbox => {
        checkbox.addEventListener('change', function() {
            const selectedMonths = getSelectedMonths('industry-month-selector');
            if (selectedMonths && selectedMonths.length > 1) {
                modeGroup.style.display = 'block';
            } else {
                modeGroup.style.display = 'none';
            }
        });
    });
}

// 存储原始掩码值，用于判断用户是否修改了API密钥
let maskedApiKeys = {
    tushare: null,
    finnhub: null
};

// 检查字符串是否是掩码格式（包含*）
function isMaskedValue(value) {
    return value && value.includes('*');
}

// 加载配置
async function loadConfig() {
    try {
        const response = await fetch('/api/config');
        const config = await response.json();
        
        // 设置数据源配置
        if (config.data_source) {
            const ds = config.data_source;
            document.getElementById('config-baostock').checked = ds.baostock?.enabled || false;
            document.getElementById('config-tushare').checked = ds.tushare?.enabled || false;
            
            // API密钥显示为掩码
            const tushareMasked = ds.tushare?.api_key || '';
            const tushareInput = document.getElementById('config-tushare-token');
            tushareInput.value = tushareMasked;
            tushareInput.type = 'password'; // 使用password类型进一步保护
            maskedApiKeys.tushare = tushareMasked;
            
            // 添加焦点事件：如果当前值是掩码，获得焦点时清空
            tushareInput.addEventListener('focus', function() {
                if (isMaskedValue(this.value)) {
                    this.value = '';
                }
            });
            
            document.getElementById('config-finnhub').checked = ds.finnhub?.enabled || false;
            const finnhubMasked = ds.finnhub?.api_key || '';
            const finnhubInput = document.getElementById('config-finnhub-key');
            finnhubInput.value = finnhubMasked;
            finnhubInput.type = 'password'; // 使用password类型进一步保护
            maskedApiKeys.finnhub = finnhubMasked;
            
            // 添加焦点事件：如果当前值是掩码，获得焦点时清空
            finnhubInput.addEventListener('focus', function() {
                if (isMaskedValue(this.value)) {
                    this.value = '';
                }
            });
            
            document.getElementById('config-akshare').checked = ds.akshare?.enabled || false;
        }
    } catch (error) {
        console.error('加载配置失败:', error);
    }
}

document.addEventListener('DOMContentLoaded', function() {
    setupMonthSelectorListener();
    setupIndustryMonthSelectorListener();
    loadIndustries();
    loadConfig();
});

