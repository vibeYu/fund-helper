# 基金估值助手 (Fund Helper)

轻量级基金估值追踪工具：盘中估值实时刷新、收盘净值自动入库、策略告警推送、市场行情概览。

零配置 SQLite 存储，无需注册登录，开箱即用。

## 功能概览

### 市场行情首页
- **大盘指数**：实时展示上证、深证、创业板、北证50，点击查看分时/日K走势
- **行业热力图**：申万行业 Treemap，红涨绿跌一目了然
- **行业涨跌排行**：涨幅榜 + 跌幅榜 Top5
- **主力资金流向**：行业资金流入/流出 Top10
- **热门概念词云**：基于同花顺热股概念标签的实时词云

### 估值与净值
- **盘中估值**：交易时段自动抓取基金实时估值（支持 interval / 定时两种模式）
- **收盘净值**：盘后自动获取官方净值并替换估值显示
- **历史净值**：支持查看 180 天内净值走势 + MA5/MA15/MA30 均线

### 策略告警
- **均线策略 (MA)**：净值穿越均线时触发（支持加仓/减仓方向）
- **阈值策略**：盘中估算涨跌幅超阈值时触发
- **趋势线策略**：基于 zigzag 极值的趋势线突破检测
- 告警自动去重（每基金每天每方向仅触发一次）

### 通知推送
配置即启用，支持多渠道同时推送：
- 企业微信 Webhook
- 飞书 Webhook
- Telegram Bot
- 邮件（SMTP，自动识别 QQ/163/Gmail 等）
- 自定义 Webhook（钉钉/Slack/Discord 等）

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置

```bash
cp .env.example .env
```

编辑 `.env`，至少配置基金列表：

```ini
# 基金代码（逗号分隔）
FUND_LIST=161725,110011,110020

# 基金简称（可选）
FUND_ALIASES=161725:白酒,110011:中小盘
```

### 3. 运行

```bash
# 抓取一次估值（验证配置是否正确）
python main.py

# 启动 Web 服务
python main.py --web

# Web + 调度同时运行（推荐）
python main.py --web --schedule

# 调度模式（无 Web，纯后台）
python main.py --schedule

# 仅获取收盘净值
python main.py --eod
```

启动后访问 http://localhost:5000 查看估值看板。

### Docker 部署

```bash
cp .env.example .env
# 编辑 .env 配置
docker compose up -d
```

## 项目结构

```
fund-helper-open/
├── main.py                              # 入口：CLI 解析、日志、启动
├── docs/                                # 策略文档（Markdown）
├── data/                                # SQLite 数据库（自动创建）
├── logs/                                # 运行日志
├── src/
│   ├── config.py                        # 单例配置（.env 加载）
│   ├── trading_calendar.py              # 交易日历（时段判断、节假日）
│   ├── storage.py                       # SQLAlchemy ORM + SQLite（9张表）
│   ├── scheduler.py                     # 交易时段感知调度器
│   ├── pipeline.py                      # 业务编排（估值/净值/告警）
│   ├── notification.py                  # 多渠道通知推送
│   ├── trendline.py                     # 趋势线/支撑阻力线检测
│   ├── data_provider/
│   │   ├── base.py                      # 数据源抽象基类 + 管理器
│   │   ├── eastmoney_fetcher.py         # 天天基金 JSONP
│   │   ├── eastmoney_detail_fetcher.py  # 天天基金移动端
│   │   ├── danjuan_fetcher.py           # 蛋卷基金
│   │   ├── akshare_fetcher.py           # akshare 兜底
│   │   └── index_fetcher.py             # 大盘指数/行业板块/资金流向
│   └── web/
│       ├── app.py                       # Flask 应用工厂 + 路由
│       ├── templates/                   # Jinja2 模板
│       └── static/                      # CSS 样式
├── requirements.txt
├── Dockerfile
└── docker-compose.yml
```

## 数据源

基金估值数据按优先级自动切换，单个源失败自动 fallback：

| 优先级 | 数据源 | 说明 |
|--------|--------|------|
| 1 | 天天基金 JSONP | 盘中估值，最快 |
| 2 | 天天基金移动端 | 备用估值源 |
| 3 | 蛋卷基金 | 备用估值 + 历史净值 |
| 4 | akshare | 兜底，也用于历史净值 |

市场行情数据：

| 数据 | 来源 |
|------|------|
| 大盘指数实时/日线 | 东方财富 push2 API |
| 行业板块涨跌/资金流向 | 东方财富行业板块 API |
| 热门概念词云 | 同花顺热股 API |

## 调度模式说明

`--schedule` 模式下，调度器自动感知交易时段：

| 时段 | 行为 |
|------|------|
| 盘前 (< 9:30) | 等待开盘 |
| 盘中 (9:30 - 15:00) | 按 interval 或定时触发抓取估值 + 告警检测 |
| 盘后 (15:30) | 获取官方净值 + 指数日线 + 策略评估 + 数据清理 |
| 非交易日 | 休眠到下个交易日 |

两种盘中模式（通过 `.env` 配置）：
- **interval 模式**（默认）：每 `REFRESH_INTERVAL` 秒轮询一次
- **定时模式**：配置 `SCHEDULE_TIMES=11:30,14:30` 仅在指定时间点触发

## 策略管理

通过 API 管理策略，无需登录：

```bash
# 创建均线策略
curl -X POST http://localhost:5000/api/strategies \
  -H 'Content-Type: application/json' \
  -d '{"fund_code":"161725","strategy_type":"ma","params":{"ma_period":5,"action":"加仓"}}'

# 创建阈值策略
curl -X POST http://localhost:5000/api/strategies \
  -H 'Content-Type: application/json' \
  -d '{"fund_code":"161725","strategy_type":"threshold","params":{"rise_pct":2,"drop_pct":-2}}'

# 创建趋势线策略
curl -X POST http://localhost:5000/api/strategies \
  -H 'Content-Type: application/json' \
  -d '{"fund_code":"161725","strategy_type":"trend_line","params":{"lookback_days":90}}'

# 查看所有策略
curl http://localhost:5000/api/strategies

# 更新策略
curl -X PUT http://localhost:5000/api/strategies/1 \
  -H 'Content-Type: application/json' \
  -d '{"is_enabled":false}'

# 删除策略
curl -X DELETE http://localhost:5000/api/strategies/1
```

## 通知配置

在 `.env` 中配置，配了哪个就推送哪个，支持同时多渠道：

```ini
# 企业微信
WECHAT_WEBHOOK_URL=https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxx

# 飞书
FEISHU_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/xxx

# Telegram
TELEGRAM_BOT_TOKEN=123456:ABC-DEF
TELEGRAM_CHAT_ID=123456789

# 邮件（QQ 邮箱示例，使用授权码）
EMAIL_SENDER=your@qq.com
EMAIL_PASSWORD=smtp授权码
EMAIL_RECEIVERS=receiver@gmail.com

# 自定义 Webhook（钉钉/Slack/Discord 等自动识别格式）
CUSTOM_WEBHOOK_URLS=https://oapi.dingtalk.com/robot/send?access_token=xxx
```

## API 列表

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 估值首页 |
| GET | `/docs` | 策略文档 |
| GET | `/api/valuations` | 最新估值数据 |
| GET | `/api/fund/<code>/history` | 基金历史净值 + 均线 |
| GET | `/api/fund/<code>/intraday` | 基金盘中分时 |
| GET | `/api/fund/<code>/trendlines` | 趋势线检测 |
| GET | `/api/market/indices` | 大盘指数行情 |
| GET | `/api/market/heatmap` | 行业热力图 |
| GET | `/api/market/sectors` | 行业涨跌排行 |
| GET | `/api/market/fund-flow` | 行业资金流向 |
| GET | `/api/market/hot-concepts` | 热门概念词云 |
| GET | `/api/market/index/<code>/intraday` | 指数分时 |
| GET | `/api/market/index/<code>/history` | 指数日线 |
| GET | `/api/strategies` | 获取所有策略 |
| POST | `/api/strategies` | 创建策略 |
| PUT | `/api/strategies/<id>` | 更新策略 |
| DELETE | `/api/strategies/<id>` | 删除策略 |

## 技术栈

- **后端**：Python 3.10+ / Flask / SQLAlchemy / SQLite
- **前端**：原生 JS / ECharts（图表）/ echarts-wordcloud（词云）
- **数据源**：东方财富 / 天天基金 / 蛋卷基金 / 同花顺 / akshare
- **部署**：Docker / 直接运行

## 许可

MIT License

数据仅供参考，不构成投资建议。
