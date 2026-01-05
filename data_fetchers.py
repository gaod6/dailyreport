# data_fetchers.py
import yfinance as yf
import pandas as pd
import ccxt
import akshare as ak
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

        # === 调试日志（保留） ===
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
        # 关键修复5：移除多余的tz_convert，yfinance已正确处理时区
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

        # === 调试日志（保留） ===
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