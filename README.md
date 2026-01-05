# 项目名称

> 一个用于收集加密货币和股票日线数据的 Python 程序。

---

## 功能

- 获取加密货币（如 BTC、ETH）的日线数据
- 获取美股、A 股指数或 ETF 的日线数据
- 输出每日高低点、振幅、涨跌幅等数据，形成中文的日报，避免自行统计

## 环境与依赖

- Python 3.10+
- 必须安装以下库：
  ```bash
  pip install pandas numpy ccxt yfinance akshare pytz exchange-calendars
