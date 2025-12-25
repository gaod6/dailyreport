# data_fetchers.py
import yfinance as yf
import pandas as pd
import ccxt
import akshare as ak
import requests
from datetime import datetime, date, time
import pytz
from typing import Optional, Tuple

from utils import retry_fetch

def fetch_yf_history(symbol: str) -> Optional[pd.DataFrame]:
    def _inner():
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="60d", interval="1d", actions=True, auto_adjust=False)
        if len(df) < 2:
            return None

        # === 恢复原始调试日志 ===
        print(f"→ yf {symbol} 数据最后三行:")
        print(df.tail(3))
        print("列名:", list(df.columns))
        # ===========================

        required = ["Open", "High", "Low", "Close"]
        if "Dividends" not in df.columns:
            df["Dividends"] = 0.0
        return df[required + ["Dividends"]]

    return retry_fetch(_inner, success_msg=f"yf {symbol} 日线数据获取成功")

def fetch_stock_1m_high_low_time(symbol: str, target_date: date) -> Tuple[str, str]:
    def _inner():
        ticker = yf.Ticker(symbol)
        df = ticker.history(period="7d", interval="1m", prepost=False)
        if df.empty:
            return None
        df.index = df.index.tz_convert("US/Eastern")
        df = df[(df.index.time >= time(9, 30)) & (df.index.time <= time(16, 0))]
        df = df.reset_index().rename(columns={"Datetime": "time"})
        day_df = df[pd.to_datetime(df["time"]).dt.date == target_date]
        if day_df.empty:
            return None
        high_t = day_df.loc[day_df["High"].idxmax(), "time"].strftime("%H:%M")
        low_t = day_df.loc[day_df["Low"].idxmin(), "time"].strftime("%H:%M")
        return (high_t, low_t)
    result = retry_fetch(_inner, success_msg=f"{symbol} 分钟极值时间获取成功")
    return result if result else ("", "")

def fetch_ak_index(ak_symbol: str, source_type: str) -> Optional[pd.DataFrame]:
    def _inner():
        if source_type == "ak_hk":
            df_raw = ak.stock_hk_index_daily_em(symbol=ak_symbol)
        else:
            df_raw = ak.index_global_hist_em(symbol=ak_symbol)

        # === 恢复原始调试日志 ===
        print(f"→ ak {ak_symbol} ({'港股指数' if source_type == 'ak_hk' else '全球指数'}) 数据最后三行:")
        print(df_raw.tail(3) if not df_raw.empty else "（空数据）")
        print("列名:", list(df_raw.columns) if not df_raw.empty else "empty")
        # ===========================

        if df_raw is None or df_raw.empty or len(df_raw) < 2:
            return None

        rename_dict = {"日期": "date", "今开": "Open", "最高": "High", "最低": "Low"}
        possible_close = ["最新价", "收盘", "close", "Close", "latest", "price", "last"]
        for col in possible_close:
            if col in df_raw.columns:
                rename_dict[col] = "Close"
                break
        eng_map = {"open": "Open", "high": "High", "low": "Low", "close": "Close", "latest": "Close"}
        for col in df_raw.columns:
            if col.lower() in eng_map:
                rename_dict[col] = eng_map[col.lower()]

        df_raw = df_raw.rename(columns=rename_dict)
        if "date" in df_raw.columns:
            df_raw["date"] = pd.to_datetime(df_raw["date"])
            df_raw = df_raw.set_index("date")

        required = ["Open", "High", "Low", "Close"]
        if not all(c in df_raw.columns for c in required):
            print(f"  → 缺失必要列: {required}，实际列: {list(df_raw.columns)}")
            return None
        df_raw["Dividends"] = 0.0
        return df_raw[required + ["Dividends"]]

    msg = f"ak {ak_symbol} ({'港股指数' if source_type == 'ak_hk' else '全球指数'}) 数据获取成功"
    return retry_fetch(_inner, success_msg=msg)

def fetch_crypto_daily(symbol: str, name: str) -> Tuple[date, float, float, float, float]:
    def _inner():
        exchange = ccxt.binance({'enableRateLimit': True})
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1d', limit=3)
        if len(ohlcv) < 3:
            return None
        prev = ohlcv[-2]
        dt = datetime.fromtimestamp(prev[0] / 1000, tz=pytz.UTC).date()
        return dt, round(prev[1], 2), round(prev[2], 2), round(prev[3], 2), round(prev[4], 2)
    result = retry_fetch(_inner, success_msg=f"{name} 日线数据获取成功")
    if result is None:
        return date.today(), 0.0, 0.0, 0.0, 0.0
    return result

def fetch_crypto_high_low_time(symbol: str, target_start_ms: int, name: str) -> Tuple[str, str]:
    def _inner():
        exchange = ccxt.binance({'enableRateLimit': True})
        all_ohlcv = []
        since = target_start_ms
        for _ in range(3):
            batch = exchange.fetch_ohlcv(symbol, '1m', since=since, limit=720)
            if not batch:
                break
            all_ohlcv.extend(batch)
            since = batch[-1][0] + 60000
        df = pd.DataFrame(all_ohlcv, columns=["ts", "O", "H", "L", "C", "V"])
        df = df[df["ts"] < target_start_ms + 86400000]
        if len(df) < 1380:
            return None
        df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.tz_convert("Asia/Shanghai")
        high_t = df.loc[df["H"].idxmax(), "time"].strftime("%H:%M")
        low_t = df.loc[df["L"].idxmin(), "time"].strftime("%H:%M")
        return (high_t, low_t)
    result = retry_fetch(_inner, success_msg=f"{name} 分钟极值时间获取成功")
    return result if result else ("", "")

def fetch_matching_fng(crypto_report_date: date) -> Tuple[int, str]:
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get("https://api.alternative.me/fng/?limit=3", headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()["data"]
        if len(data) < 2:
            raise ValueError("数据不足")

        yesterday_value = int(data[1]["value"])
        yesterday_classification_eng = data[1]["value_classification"]
        yesterday_timestamp = int(data[1]["timestamp"])
        yesterday_api_date = datetime.utcfromtimestamp(yesterday_timestamp).date()

        if yesterday_api_date == crypto_report_date:
            classification_map = {
                "Extreme Fear": "极端恐惧",
                "Fear": "恐惧",
                "Neutral": "中性",
                "Greed": "贪婪",
                "Extreme Greed": "极端贪婪",
            }
            classification = classification_map.get(yesterday_classification_eng, yesterday_classification_eng)
            print(f"→ 恐贪指数获取成功：{yesterday_value} ({classification})")
            return yesterday_value, classification
        else:
            return 0, "未知（日期不匹配）"
    except Exception as e:
        print(f"→ 获取恐贪指数失败: {e}")
        return 0, "未知"

def fetch_matching_cnn_fng(us_report_date: date) -> Tuple[int, str]:
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.cnn.com/markets/fear-and-greed',
            'Origin': 'https://www.cnn.com',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()

        if not response.text.strip().startswith('{'):
            print(f"→ CNN API 返回非JSON内容（可能是被拦截）：{response.text[:200]}")
            return 0, "未知（接口被拦截）"

        data = response.json()
        historical_data = data.get("fear_and_greed_historical", {}).get("data", [])
        if not historical_data:
            raise ValueError("无历史数据")

        rating_map = {
            "extreme fear": "极端恐惧",
            "fear": "恐惧",
            "neutral": "中性",
            "greed": "贪婪",
            "extreme greed": "极端贪婪",
        }

        for entry in historical_data:
            entry_date = datetime.fromtimestamp(entry["x"] / 1000).date()
            if entry_date == us_report_date:
                value = round(entry["y"])
                eng_rating = entry["rating"]
                classification = rating_map.get(eng_rating, "未知")
                print(f"→ 股市恐贪指数获取成功：{value} ({classification})")
                return value, classification

        print(f"→ 未找到匹配日期 {us_report_date} 的恐贪指数")
        return 0, "未知（无匹配日期）"

    except requests.exceptions.RequestException as e:
        print(f"→ 获取股市恐贪指数网络失败: {e}")
        return 0, "未知（网络错误）"
    except ValueError as e:
        print(f"→ 获取股市恐贪指数解析失败: {e}")
        return 0, "未知（解析错误）"
    except Exception as e:
        print(f"→ 获取股市恐贪指数未知错误: {type(e).__name__}: {e}")
        return 0, "未知"