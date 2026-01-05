# processor.py
from datetime import date, datetime
from typing import List, Optional, Tuple, Dict
import pandas as pd
import pytz

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

    if isinstance(df.index, pd.DatetimeIndex):
        completed_df = df[df.index.date <= completed_date]
    else:
        return None, None, None, "stale"

    if len(completed_df) < 2:
        return None, None, None, "stale"

    last_row = completed_df.iloc[-1]
    prev_row = completed_df.iloc[-2]

    latest_day = {
        "High": round(float(last_row["High"]), 2),
        "Low": round(float(last_row["Low"]), 2),
        "Close": round(float(last_row["Close"]), 2),
    }

    prev_close = float(prev_row["Close"])
    dividend_today = float(last_row.get("Dividends", 0.0))
    base_close = prev_close - dividend_today if result_type == "stock" and dividend_today > 0 else prev_close

    trading_date = last_row.name.date() if isinstance(last_row.name, (pd.Timestamp, datetime)) else None

    return latest_day, base_close, trading_date, "ok"


def collect_data() -> Tuple[List[DailyResult], Optional[date], Optional[date]]:
    results = []
    crypto_report_date = None
    us_report_date = get_latest_completed_session("XNYS", "yf")

    for name, cfg in MARKETS.items():
        print(f"\n获取中：{name}")

        if cfg["source"] == "binance":
            daily_result = fetch_crypto_daily(cfg["symbol"], name)
            if daily_result is None:
                results.append(DailyResult(
                    name=name, high=None, high_time="", high_pct=None, low=None, low_time="", low_pct=None,
                    close=None, close_pct=None, amplitude_pct=None, result_type="crypto",
                    date_str="", status="stale"
                ))
                continue

            dt, open_p, high, low, close = daily_result
            if crypto_report_date is None:
                crypto_report_date = dt

            start_ms = int(datetime.combine(dt, datetime.min.time())
                           .replace(tzinfo=pytz.UTC).timestamp() * 1000)
            high_low_time = fetch_crypto_high_low_time(cfg["symbol"], start_ms, name)
            high_t, low_t = high_low_time if high_low_time else ("", "")

            status = "ok" if (high_t and high_t != "" and low_t and low_t != "") else "partial"

            results.append(DailyResult(
                name=name, high=high, high_time=high_t,
                high_pct=round((high - open_p) / open_p * 100, 2) if open_p and open_p != 0 else None,
                low=low, low_time=low_t,
                low_pct=round((low - open_p) / open_p * 100, 2) if open_p and open_p != 0 else None,
                close=close,
                close_pct=round((close - open_p) / open_p * 100, 2) if open_p and open_p != 0 else None,
                amplitude_pct=round((high - low) / open_p * 100, 2) if open_p and open_p != 0 else None,
                result_type="crypto", date_str=format_date_display(dt), status=status
            ))
            continue

        calendar = cfg.get("calendar")
        source = cfg["source"]
        df = (fetch_yf_history(cfg["symbol"]) if source == "yf"
              else fetch_ak_index(cfg["ak_symbol"], "ak_hk" if source == "ak_hk" else "ak"))

        latest_day, base_close, trading_date, status = get_latest_day_data(df, calendar, source, cfg["type"])

        if latest_day is None:
            results.append(DailyResult(
                name=name, high=None, high_time="", high_pct=None, low=None, low_time="", low_pct=None,
                close=None, close_pct=None, amplitude_pct=None, result_type=cfg["type"],
                date_str="", status="stale"
            ))
            continue

        high_t, low_t = ("", "")
        if cfg["type"] == "stock":
            high_t, low_t = fetch_stock_1m_high_low_time(cfg["symbol"], trading_date)

        results.append(DailyResult(
            name=name,
            high=latest_day["High"], high_time=high_t,
            high_pct=round((latest_day["High"] - base_close) / base_close * 100, 2) if base_close and base_close != 0 else None,
            low=latest_day["Low"], low_time=low_t,
            low_pct=round((latest_day["Low"] - base_close) / base_close * 100, 2) if base_close and base_close != 0 else None,
            close=latest_day["Close"],
            close_pct=round((latest_day["Close"] - base_close) / base_close * 100, 2) if base_close and base_close != 0 else None,
            amplitude_pct=round((latest_day["High"] - latest_day["Low"]) / base_close * 100, 2) if base_close and base_close != 0 else None,
            result_type=cfg["type"],
            date_str=format_date_display(trading_date) if trading_date else "",
            status=status
        ))

    return results, crypto_report_date, us_report_date


def post_process(results: List[DailyResult]) -> List[DailyResult]:
    order_map = {name: i for i, name in enumerate(DISPLAY_ORDER)}
    max_order = max(order_map.values()) if order_map else -1
    fallback_order = max_order + 1
    return sorted(results, key=lambda x: order_map.get(x.name, fallback_order))