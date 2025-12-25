# processor.py
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple, Dict
import pandas as pd
import pytz
import numpy as np
import ccxt  # 用于加密货币数据获取

from models import DailyResult
from config import MARKETS, DISPLAY_ORDER
from utils import format_date_display, get_latest_completed_session
from data_fetchers import (
    fetch_yf_history, fetch_ak_index, fetch_stock_1m_high_low_time,
    fetch_crypto_daily, fetch_crypto_high_low_time
)

def get_latest_day_data(df: pd.DataFrame, calendar_name: Optional[str], source: str, result_type: str
                        ) -> Tuple[Optional[Dict], Optional[float], Optional[date], str]:
    if df is None or df.empty or len(df) < 2:
        return None, None, None, "stale"

    df = df.sort_index(ascending=True)
    completed_date = get_latest_completed_session(calendar_name, source)
    if completed_date is None:
        return None, None, None, "stale"

    last_row = df.iloc[-1]
    if isinstance(last_row.name, (pd.Timestamp, datetime)):
        last_date = last_row.name.date()
    else:
        last_date = None

    if last_date and last_date > completed_date and len(df) >= 3:
        print(f"  → 检测到不完整当天数据（{last_date} > {completed_date}），回退至前一日")
        last_row = df.iloc[-2]
        prev_row = df.iloc[-3]
        if isinstance(last_row.name, (pd.Timestamp, datetime)):
            last_date = last_row.name.date()
    else:
        prev_row = df.iloc[-2]

    if last_date is None or last_date > completed_date:
        return None, None, None, "stale"

    latest_day = {
        "High": round(float(last_row["High"]), 2),
        "Low": round(float(last_row["Low"]), 2),
        "Close": round(float(last_row["Close"]), 2),
    }

    prev_close = float(prev_row["Close"])
    dividend_today = float(last_row.get("Dividends", 0.0))
    base_close = prev_close - dividend_today if result_type == "stock" and dividend_today > 0 else prev_close

    return latest_day, base_close, last_date, "ok"


def calc_hv_components(log_returns: pd.Series, ddof: int = 0) -> Tuple[float, float, float, float]:
    """
    通用函数：计算总、上行、下行波动率及其比值（年化前）
    """
    total = log_returns.std(ddof=ddof)
    up = log_returns[log_returns > 0].std(ddof=ddof)
    down = np.abs(log_returns[log_returns < 0]).std(ddof=ddof)
    ratio = up / down if down > 0 else 0.0
    return total, up, down, ratio


def calculate_crypto_volatility(symbol: str, report_date: date) -> Dict[str, float]:
    """
    计算基于收盘价的波动率，包括总、上行、下行、比值，以及变化
    返回字典，包含所有指标
    """
    try:
        exchange = ccxt.binance({'enableRateLimit': True})
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1d', limit=100)
        if len(ohlcv) < 31:
            print(f"  → {symbol} 数据不足（仅{len(ohlcv)}根），无法计算波动率")
            return {k: 0.0 for k in ['hv7', 'hv7_change', 'hv7_up', 'hv7_up_change', 'hv7_down', 'hv7_down_change', 'hv7_ratio', 'hv7_ratio_change',
                                      'hv30', 'hv30_change', 'hv30_up', 'hv30_up_change', 'hv30_down', 'hv30_down_change', 'hv30_ratio', 'hv30_ratio_change']}

        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.date
        df = df.sort_values("date").reset_index(drop=True)

        df_valid = df[df["date"] <= report_date]
        if df_valid.empty or len(df_valid) < 2:
            print(f"  → {symbol} 无已完成日线")
            return {k: 0.0 for k in ['hv7', 'hv7_change', 'hv7_up', 'hv7_up_change', 'hv7_down', 'hv7_down_change', 'hv7_ratio', 'hv7_ratio_change',
                                      'hv30', 'hv30_change', 'hv30_up', 'hv30_up_change', 'hv30_down', 'hv30_down_change', 'hv30_ratio', 'hv30_ratio_change']}

        target_idx = df_valid.index[-1]

        res = {}
        # 7日
        if target_idx >= 6:
            closes_7 = df.iloc[target_idx - 6: target_idx + 1]["close"].values
            log_ret_7 = np.log(pd.Series(closes_7) / pd.Series(closes_7).shift(1)).dropna()
            total, up, down, ratio = calc_hv_components(log_ret_7)
            res['hv7'] = round(total * np.sqrt(365) * 100, 1)
            res['hv7_up'] = round(up * np.sqrt(365) * 100, 1)
            res['hv7_down'] = round(down * np.sqrt(365) * 100, 1)
            res['hv7_ratio'] = round(ratio, 2)

            prev_idx = target_idx - 1
            if prev_idx >= 6:
                prev_closes_7 = df.iloc[prev_idx - 6: prev_idx + 1]["close"].values
                prev_log_7 = np.log(pd.Series(prev_closes_7) / pd.Series(prev_closes_7).shift(1)).dropna()
                prev_total, prev_up, prev_down, prev_ratio = calc_hv_components(prev_log_7)
                res['hv7_change'] = round(res['hv7'] - round(prev_total * np.sqrt(365) * 100, 1), 1)
                res['hv7_up_change'] = round(res['hv7_up'] - round(prev_up * np.sqrt(365) * 100, 1), 1)
                res['hv7_down_change'] = round(res['hv7_down'] - round(prev_down * np.sqrt(365) * 100, 1), 1)
                res['hv7_ratio_change'] = round(res['hv7_ratio'] - round(prev_ratio, 2), 2)
            else:
                res['hv7_change'] = res['hv7_up_change'] = res['hv7_down_change'] = res['hv7_ratio_change'] = 0.0
        else:
            res['hv7'] = res['hv7_change'] = res['hv7_up'] = res['hv7_up_change'] = res['hv7_down'] = res['hv7_down_change'] = res['hv7_ratio'] = res['hv7_ratio_change'] = 0.0
            print(f"  → {symbol} 7日数据不足（仅{target_idx + 1}天）")

        # 30日
        if target_idx >= 29:
            closes_30 = df.iloc[target_idx - 29: target_idx + 1]["close"].values
            log_ret_30 = np.log(pd.Series(closes_30) / pd.Series(closes_30).shift(1)).dropna()
            total, up, down, ratio = calc_hv_components(log_ret_30)
            res['hv30'] = round(total * np.sqrt(365) * 100, 1)
            res['hv30_up'] = round(up * np.sqrt(365) * 100, 1)
            res['hv30_down'] = round(down * np.sqrt(365) * 100, 1)
            res['hv30_ratio'] = round(ratio, 2)

            prev_idx = target_idx - 1
            if prev_idx >= 29:
                prev_closes_30 = df.iloc[prev_idx - 29: prev_idx + 1]["close"].values
                prev_log_30 = np.log(pd.Series(prev_closes_30) / pd.Series(prev_closes_30).shift(1)).dropna()
                prev_total, prev_up, prev_down, prev_ratio = calc_hv_components(prev_log_30)
                res['hv30_change'] = round(res['hv30'] - round(prev_total * np.sqrt(365) * 100, 1), 1)
                res['hv30_up_change'] = round(res['hv30_up'] - round(prev_up * np.sqrt(365) * 100, 1), 1)
                res['hv30_down_change'] = round(res['hv30_down'] - round(prev_down * np.sqrt(365) * 100, 1), 1)
                res['hv30_ratio_change'] = round(res['hv30_ratio'] - round(prev_ratio, 2), 2)
            else:
                res['hv30_change'] = res['hv30_up_change'] = res['hv30_down_change'] = res['hv30_ratio_change'] = 0.0
        else:
            res['hv30'] = res['hv30_change'] = res['hv30_up'] = res['hv30_up_change'] = res['hv30_down'] = res['hv30_down_change'] = res['hv30_ratio'] = res['hv30_ratio_change'] = 0.0
            print(f"  → {symbol} 30日数据不足（仅{target_idx + 1}天）")

        return res

    except Exception as e:
        print(f"→ 计算 {symbol} 波动率失败: {e}")
        return {k: 0.0 for k in ['hv7', 'hv7_change', 'hv7_up', 'hv7_up_change', 'hv7_down', 'hv7_down_change', 'hv7_ratio', 'hv7_ratio_change',
                                  'hv30', 'hv30_change', 'hv30_up', 'hv30_up_change', 'hv30_down', 'hv30_down_change', 'hv30_ratio', 'hv30_ratio_change']}


def calculate_parkinson_hv(symbol: str, report_date: date, window: int = 30) -> Dict[str, float]:
    """
    计算基于盘中波动的波动率，包括总、上行、下行、比值，以及变化
    返回字典，包含所有指标
    """
    try:
        exchange = ccxt.binance({'enableRateLimit': True})
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe='1d', limit=window + 5)

        df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'vol'])
        df['date'] = pd.to_datetime(df['ts'], unit='ms', utc=True).dt.date
        df = df.sort_values('date').reset_index(drop=True)

        target_rows = df[df['date'] == report_date]
        if target_rows.empty:
            print(f"  → {symbol} 未找到报告日期 {report_date} 的数据")
            return {'hv': 0.0, 'hv_change': 0.0, 'hv_up': 0.0, 'hv_up_change': 0.0, 
                    'hv_down': 0.0, 'hv_down_change': 0.0, 'ratio': 0.0, 'ratio_change': 0.0}

        target_idx = target_rows.index[0]

        # 允许部分窗口，如果不足用可用数据
        effective_window = min(window, target_idx + 1)
        if effective_window < 2:
            print(f"  → {symbol} {window}日数据不足（仅{target_idx + 1}天）")
            return {'hv': 0.0, 'hv_change': 0.0, 'hv_up': 0.0, 'hv_up_change': 0.0, 
                    'hv_down': 0.0, 'hv_down_change': 0.0, 'ratio': 0.0, 'ratio_change': 0.0}

        # 关键修复：先创建 log_hl 列
        df['log_hl'] = np.log(df['high'] / df['low'])

        # 计算总 Parkinson HV
        rolling_mean_all = df['log_hl'].rolling(window=effective_window).mean()
        hv = round(float(np.sqrt(rolling_mean_all.iloc[target_idx] / (4 * np.log(2))) * np.sqrt(365) * 100), 1)

        # 计算上行/下行（使用当日收盘涨跌方向）
        df['log_ret'] = np.log(df['close'] / df['close'].shift(1))

        df['log_hl_up'] = df['log_hl'].where(df['log_ret'] > 0, 0)
        df['log_hl_down'] = df['log_hl'].where(df['log_ret'] < 0, 0)

        rolling_mean_up = df['log_hl_up'].rolling(window=effective_window).mean()
        rolling_mean_down = df['log_hl_down'].rolling(window=effective_window).mean()

        hv_up = round(float(np.sqrt(rolling_mean_up.iloc[target_idx] / (4 * np.log(2))) * np.sqrt(365) * 100), 1)
        hv_down = round(float(np.sqrt(rolling_mean_down.iloc[target_idx] / (4 * np.log(2))) * np.sqrt(365) * 100), 1)
        ratio = hv_up / hv_down if hv_down > 0 else 0.0
        ratio = round(ratio, 2)

        # 变化
        hv_change = hv_up_change = hv_down_change = ratio_change = 0.0
        prev_idx = target_idx - 1
        prev_effective_window = min(window, prev_idx + 1)
        if prev_effective_window >= 2:
            prev_hv = float(np.sqrt(rolling_mean_all.iloc[prev_idx] / (4 * np.log(2))) * np.sqrt(365) * 100)
            prev_hv_up = float(np.sqrt(rolling_mean_up.iloc[prev_idx] / (4 * np.log(2))) * np.sqrt(365) * 100)
            prev_hv_down = float(np.sqrt(rolling_mean_down.iloc[prev_idx] / (4 * np.log(2))) * np.sqrt(365) * 100)
            prev_ratio = prev_hv_up / prev_hv_down if prev_hv_down > 0 else 0.0

            hv_change = round(hv - prev_hv, 1)
            hv_up_change = round(hv_up - prev_hv_up, 1)
            hv_down_change = round(hv_down - prev_hv_down, 1)
            ratio_change = round(ratio - prev_ratio, 2)

        return {'hv': hv, 'hv_change': hv_change, 'hv_up': hv_up, 'hv_up_change': hv_up_change, 
                'hv_down': hv_down, 'hv_down_change': hv_down_change, 'ratio': ratio, 'ratio_change': ratio_change}

    except Exception as e:
        print(f"→ 计算 {symbol} Parkinson 波动率失败: {e}")
        return {'hv': 0.0, 'hv_change': 0.0, 'hv_up': 0.0, 'hv_up_change': 0.0, 
                'hv_down': 0.0, 'hv_down_change': 0.0, 'ratio': 0.0, 'ratio_change': 0.0}


def collect_data() -> Tuple[List[DailyResult], Optional[date], Optional[date]]:
    results = []
    crypto_date_str = ""
    crypto_report_date = None
    us_report_date = get_latest_completed_session("XNYS", "yf")

    for name, cfg in MARKETS.items():
        print(f"\n获取中：{name}")

        if cfg["source"] == "binance":
            dt, open_p, high, low, close = fetch_crypto_daily(cfg["symbol"], name)
            if not crypto_date_str:
                crypto_date_str = format_date_display(dt)
                crypto_report_date = dt

            start_ms = int(datetime.combine(dt, datetime.min.time())
                          .replace(tzinfo=pytz.UTC).timestamp() * 1000)
            high_t, low_t = fetch_crypto_high_low_time(cfg["symbol"], start_ms, name)

            # 计算 C2C 波动率
            c2c_res = calculate_crypto_volatility(cfg["symbol"], dt)

            # 计算 Parkinson 波动率（7日和30日）
            parkinson7_res = calculate_parkinson_hv(cfg["symbol"], dt, window=7)
            parkinson30_res = calculate_parkinson_hv(cfg["symbol"], dt, window=30)

            status = "partial" if not high_t else "ok"
            results.append(DailyResult(
                name=name, high=high, high_time=high_t,
                high_pct=round((high - open_p) / open_p * 100, 2) if open_p else 0,
                low=low, low_time=low_t,
                low_pct=round((low - open_p) / open_p * 100, 2) if open_p else 0,
                close=close,
                close_pct=round((close - open_p) / open_p * 100, 2) if open_p else 0,
                amplitude_pct=round((high - low) / open_p * 100, 2) if open_p else 0,
                result_type="crypto", date_str=crypto_date_str, status=status,
                hv7=c2c_res.get('hv7', 0.0), hv7_change=c2c_res.get('hv7_change', 0.0),
                hv7_up=c2c_res.get('hv7_up', 0.0), hv7_up_change=c2c_res.get('hv7_up_change', 0.0),
                hv7_down=c2c_res.get('hv7_down', 0.0), hv7_down_change=c2c_res.get('hv7_down_change', 0.0),
                hv7_ratio=c2c_res.get('hv7_ratio', 0.0), hv7_ratio_change=c2c_res.get('hv7_ratio_change', 0.0),
                hv30=c2c_res.get('hv30', 0.0), hv30_change=c2c_res.get('hv30_change', 0.0),
                hv30_up=c2c_res.get('hv30_up', 0.0), hv30_up_change=c2c_res.get('hv30_up_change', 0.0),
                hv30_down=c2c_res.get('hv30_down', 0.0), hv30_down_change=c2c_res.get('hv30_down_change', 0.0),
                hv30_ratio=c2c_res.get('hv30_ratio', 0.0), hv30_ratio_change=c2c_res.get('hv30_ratio_change', 0.0),
                parkinson_hv7=parkinson7_res.get('hv', 0.0), parkinson_hv7_change=parkinson7_res.get('hv_change', 0.0),
                parkinson_hv7_up=parkinson7_res.get('hv_up', 0.0), parkinson_hv7_up_change=parkinson7_res.get('hv_up_change', 0.0),
                parkinson_hv7_down=parkinson7_res.get('hv_down', 0.0), parkinson_hv7_down_change=parkinson7_res.get('hv_down_change', 0.0),
                parkinson_hv7_ratio=parkinson7_res.get('ratio', 0.0), parkinson_hv7_ratio_change=parkinson7_res.get('ratio_change', 0.0),
                parkinson_hv30=parkinson30_res.get('hv', 0.0), parkinson_hv30_change=parkinson30_res.get('hv_change', 0.0),
                parkinson_hv30_up=parkinson30_res.get('hv_up', 0.0), parkinson_hv30_up_change=parkinson30_res.get('hv_up_change', 0.0),
                parkinson_hv30_down=parkinson30_res.get('hv_down', 0.0), parkinson_hv30_down_change=parkinson30_res.get('hv_down_change', 0.0),
                parkinson_hv30_ratio=parkinson30_res.get('ratio', 0.0), parkinson_hv30_ratio_change=parkinson30_res.get('ratio_change', 0.0)
            ))
            continue

        # 非加密货币部分（保持不变）
        calendar = cfg.get("calendar")
        source = cfg["source"]
        df = (fetch_yf_history(cfg["symbol"]) if source == "yf"
              else fetch_ak_index(cfg["ak_symbol"], "ak_hk" if source == "ak_hk" else "ak"))

        latest_day, base_close, trading_date, status = get_latest_day_data(df, calendar, source, cfg["type"])

        if latest_day is None:
            results.append(DailyResult(
                name=name, high=0, high_time="", high_pct=0, low=0, low_time="", low_pct=0,
                close=0, close_pct=0, amplitude_pct=0, result_type=cfg["type"],
                date_str="", status="stale"
            ))
            continue

        high_t, low_t = ("", "")
        if cfg["type"] == "stock":
            high_t, low_t = fetch_stock_1m_high_low_time(cfg["symbol"], trading_date)

        results.append(DailyResult(
            name=name,
            high=latest_day["High"], high_time=high_t,
            high_pct=round((latest_day["High"] - base_close) / base_close * 100, 2),
            low=latest_day["Low"], low_time=low_t,
            low_pct=round((latest_day["Low"] - base_close) / base_close * 100, 2),
            close=latest_day["Close"],
            close_pct=round((latest_day["Close"] - base_close) / base_close * 100, 2),
            amplitude_pct=round((latest_day["High"] - latest_day["Low"]) / base_close * 100, 2),
            result_type=cfg["type"],
            date_str=format_date_display(trading_date),
            status=status
        ))

    return results, crypto_report_date, us_report_date


def post_process(results: List[DailyResult]) -> List[DailyResult]:
    order_map = {name: i for i, name in enumerate(DISPLAY_ORDER)}
    return sorted(results, key=lambda x: order_map.get(x.name, 999))